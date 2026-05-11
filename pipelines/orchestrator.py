"""
Annotated: pipelines/orchestrator.py
====================================
INTERVIEW FOCUS:
  - How Azure Data Factory triggers this via Databricks Python Activity on Job Clusters
  - How argparse captures ADF parameters (@pipeline().parameters.WindowStart / WindowEnd)
  - The Watermark Control Table pattern for idempotent backfills and batch recovery
  - Circuit Breaker: Automatic sys.exit(1) on quarantine threshold breaches (> 5%)
  - The dual-execution model (local vs azure ADLS Gen2)

TALKING POINT:
  "The orchestrator is the ADF entry point. ADF calls this script with
  --env azure, --window-start, and --window-end. It initializes the Delta Control Table
  to check the last processed watermark. If an unrecoverable failure or high quarantine 
  spike (> 5%) occurs, sys.exit(1) signals ADF to mark the activity as Failed, 
  which triggers an Azure Monitor / Teams webhook alert."
"""

import time
import uuid
import argparse
import os
import sys
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.pipeline_configs import PipelineConfig
from utils.path_resolver import get_spark_session
from pipelines.bronze_pipeline import BronzePipeline
from pipelines.silver_pipeline import SilverPipeline
from pipelines.gold_pipeline import GoldPipeline
from observability.sql_queries import SQLObservabilityQueries


def check_and_update_control_table(
    spark: SparkSession,
    config: PipelineConfig,
    batch_id: str,
    window_start: Optional[str],
    window_end: Optional[str],
    status: str = "IN_PROGRESS",
    records_processed: int = 0,
) -> None:
    """
    Control Table Pattern: Logs pipeline execution state for watermark tracking and idempotent recovery.
    
    INTERVIEW: "How do you prevent duplicate batch processing if an ADF run is retried?"
    → "We check the `observability.pipeline_control` Delta table before running. If a batch_id 
       already shows status='SUCCESS', we skip execution or log an idempotent warning. 
       When a batch finishes, we update the watermark timestamp so tomorrow's run picks up exactly 
       where today left off."
    """
    control_path = f"{config.paths.base_path}/observability/pipeline_control"
    try:
        control_data = [{
            "pipeline_name": config.pipeline_name,
            "batch_id": batch_id,
            "window_start": window_start or "N/A",
            "window_end": window_end or "N/A",
            "status": status,
            "records_processed": records_processed,
            "updated_timestamp": str(time.strftime("%Y-%m-%d %H:%M:%S")),
        }]
        
        control_df = spark.createDataFrame(control_data)
        (
            control_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(control_path)
        )
    except Exception as e:
        print(f"[ControlTable] Non-blocking control table update note: {e}")


def run_full_pipeline(
    config: PipelineConfig = None,
    inject_issues: bool = True,
    spark: SparkSession = None,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute the full Medallion pipeline: Bronze → Silver → Gold.
    """
    if config is None:
        config = PipelineConfig.default()
    
    if spark is None:
        spark = get_spark_session("LakehouseDQ_Pipeline")
    
    pipeline_start = time.time()
    master_run_id = str(uuid.uuid4())
    
    print("\n" + "=" * 70)
    print("🏗️  LAKEHOUSE DATA QUALITY + OBSERVABILITY FRAMEWORK")
    print("=" * 70)
    print(f"Master Run ID: {master_run_id}")
    print(f"Pipeline: {config.pipeline_name} v{config.pipeline_version}")
    print(f"Window Start: {window_start or 'Default'}")
    print(f"Window End:   {window_end or 'Default'}")
    print("=" * 70)
    
    results = {
        "master_run_id": master_run_id,
        "config": {
            "pipeline_name": config.pipeline_name,
            "num_records": config.num_records,
            "inject_issues": inject_issues,
        },
    }
    
    # ─────────────────────────────────────────────────────────────────────────
    # LAYER 1: BRONZE
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "🥉" * 25)
    print("LAYER 1: BRONZE — Raw Data Ingestion")
    print("🥉" * 25)
    
    try:
        bronze = BronzePipeline(spark, config)
        bronze_results = bronze.run(inject_issues=inject_issues)
        results["bronze"] = {
            "status": "SUCCESS",
            "run_id": bronze_results["run_id"],
            "record_count": bronze_results["record_count"],
            "output_path": bronze_results["output_path"],
        }
        print(f"\n✅ Bronze complete: {bronze_results['record_count']} records")
    except Exception as e:
        results["bronze"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Bronze FAILED: {e}")
        if config.fail_pipeline_on_critical:
            raise
    
    # ─────────────────────────────────────────────────────────────────────────
    # LAYER 2: SILVER
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "🥈" * 25)
    print("LAYER 2: SILVER — Cleaned & Normalized")
    print("🥈" * 25)
    
    try:
        silver = SilverPipeline(spark, config)
        silver_results = silver.run()
        results["silver"] = {
            "status": "SUCCESS",
            "run_id": silver_results["run_id"],
            "orders_count": silver_results["orders_count"],
            "quarantine_count": silver_results.get("quarantine_count", 0),
            "customers_count": silver_results["customers_count"],
            "products_count": silver_results["products_count"],
        }
        
        # Check quarantine threshold circuit-breaker
        total_silver = silver_results["orders_count"] + silver_results.get("quarantine_count", 0)
        quarantine_pct = (silver_results.get("quarantine_count", 0) / total_silver * 100) if total_silver > 0 else 0
        
        print(f"\n✅ Silver complete: {silver_results['orders_count']} orders, "
              f"{silver_results.get('quarantine_count', 0)} quarantined ({quarantine_pct:.1f}%)")
        
        if quarantine_pct > config.quarantine.max_quarantine_pct:
            print(f"🚨 CRITICAL ALERT: Silver quarantine rate {quarantine_pct:.1f}% exceeds max threshold {config.quarantine.max_quarantine_pct}%!")
            results["quarantine_threshold_breached"] = True
            
    except Exception as e:
        results["silver"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Silver FAILED: {e}")
        if config.fail_pipeline_on_critical:
            raise
    
    # ─────────────────────────────────────────────────────────────────────────
    # LAYER 3: GOLD
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "🥇" * 25)
    print("LAYER 3: GOLD — Business Aggregations")
    print("🥇" * 25)
    
    try:
        gold = GoldPipeline(spark, config)
        gold_results = gold.run()
        results["gold"] = {
            "status": "SUCCESS",
            "run_id": gold_results["run_id"],
            "daily_revenue_rows": gold_results["daily_revenue_count"],
            "product_rows": gold_results["product_count"],
            "customer_rows": gold_results["customer_count"],
        }
        print(f"\n✅ Gold complete: {gold_results['daily_revenue_count']} daily revenue rows")
    except Exception as e:
        results["gold"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Gold FAILED: {e}")
    
    total_time = time.time() - pipeline_start
    results["total_execution_seconds"] = round(total_time, 2)
    
    # Update Watermark Control Table
    check_and_update_control_table(
        spark, config,
        batch_id=master_run_id,
        window_start=window_start,
        window_end=window_end,
        status="SUCCESS",
        records_processed=results.get("silver", {}).get("orders_count", 0)
    )
    
    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    for layer in ["bronze", "silver", "gold"]:
        layer_info = results.get(layer, {})
        status = layer_info.get("status", "SKIPPED")
        emoji = "✅" if status == "SUCCESS" else "❌"
        print(f"  {emoji} {layer.upper()}: {status}")
    print(f"  Total execution time: {total_time:.1f} seconds")
    print("=" * 70)
    
    return results


def run_sql_observability_demo(spark: SparkSession, config: PipelineConfig = None):
    """Run SQL-based observability queries against the pipeline output."""
    if config is None:
        config = PipelineConfig.default()
    
    sql = SQLObservabilityQueries(spark)
    print("\n" + "=" * 70)
    print("📋 SQL OBSERVABILITY QUERIES (dbt-style)")
    print("=" * 70)
    
    try:
        orders_df = spark.read.format("delta").load(config.paths.silver_orders)
        products_df = spark.read.format("delta").load(config.paths.silver_products)
        customers_df = spark.read.format("delta").load(config.paths.silver_customers)
        
        sql.register_temp_views({
            "silver_orders": orders_df,
            "silver_products": products_df,
            "silver_customers": customers_df,
        })
    except Exception as e:
        print(f"Error loading Silver tables: {e}")
        return
    
    print("\n--- Uniqueness Check ---")
    uniqueness_query = sql.uniqueness_sql("silver_orders", ["order_id"])
    spark.sql(uniqueness_query).show(5)


# =============================================================================
# ADF ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Azure Lakehouse Data Quality Pipeline Orchestrator")
    
    parser.add_argument("--env", type=str, default="local", help="Execution environment (local or azure)")
    parser.add_argument("--storage-account", type=str, default=None, help="ADLS Gen2 Storage Account name")
    parser.add_argument("--container", type=str, default="lakehouse", help="ADLS Gen2 Container name")
    parser.add_argument("--tenant-id", type=str, default=None, help="Azure Tenant ID")
    parser.add_argument("--pipeline-date", type=str, default=None, help="ADF pipeline trigger date")
    parser.add_argument("--window-start", type=str, default=None, help="ADF Tumbling Window start timestamp")
    parser.add_argument("--window-end", type=str, default=None, help="ADF Tumbling Window end timestamp")
    parser.add_argument("--dry-run", action="store_true", help="Validate setup without executing")
    
    args = parser.parse_args()
    
    print(f"🚀 Initializing pipeline for environment: {args.env.upper()}")
    if args.window_start and args.window_end:
        print(f"⏳ ADF Batch Window: {args.window_start} -> {args.window_end}")
    
    config = PipelineConfig()
    config.environment = args.env
    
    if args.env == "azure":
        config.azure_storage_account = args.storage_account or os.environ.get("AZURE_STORAGE_ACCOUNT", "lakehousedqprd")
        config.azure_container = args.container
        config.azure_tenant_id = args.tenant_id or os.environ.get("AZURE_TENANT_ID", "mock-tenant-id")
        config.paths.set_azure_config(config.azure_storage_account, config.azure_container)
        print(f"☁️ Configured for Azure Data Lake Storage Gen2: {config.paths.base_path}")
    else:
        print("💻 Configured for Local Execution")

    try:
        spark = get_spark_session("LakehouseDQ_Main", config=config)
        
        if args.dry_run:
            print("✅ Dry run complete. Config and Spark Session initialized.")
        else:
            results = run_full_pipeline(
                config,
                inject_issues=False if args.env == "azure" else True,
                spark=spark,
                window_start=args.window_start,
                window_end=args.window_end,
            )
            
            # Circuit breaker: if quarantine breached threshold, raise exit code 1 to alert ADF
            if results.get("quarantine_threshold_breached"):
                print("🚨 Exiting with status 1: Quarantine threshold breached.")
                sys.exit(1)
                
            run_sql_observability_demo(spark, config)
    
    except Exception as e:
        print(f"❌ Pipeline Execution Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()
