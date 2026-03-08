"""
Pipeline Orchestrator — runs the full Bronze → Silver → Gold pipeline.

This module provides a single entry point for end-to-end pipeline execution,
managing dependencies between the Medallion layers and ensuring consistent
orchestration and error handling.
"""

import time
import uuid
from typing import Dict, Any

from pyspark.sql import SparkSession

from config.pipeline_configs import PipelineConfig
from utils.path_resolver import get_spark_session
from pipelines.bronze_pipeline import BronzePipeline
from pipelines.silver_pipeline import SilverPipeline
from pipelines.gold_pipeline import GoldPipeline
from observability.sql_queries import SQLObservabilityQueries


def run_full_pipeline(
    config: PipelineConfig = None,
    inject_issues: bool = True,
    spark: SparkSession = None,
) -> Dict[str, Any]:
    """
    Execute the full Medallion pipeline: Bronze → Silver → Gold.
    
    Args:
        config: Pipeline configuration (uses defaults if None)
        inject_issues: Whether to inject data quality issues in Bronze
        spark: Optional SparkSession (creates one if None)
    
    Returns:
        Combined results from all three layers
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
    print(f"Records: {config.num_records}")
    print(f"Issue Injection: {'ENABLED' if inject_issues else 'DISABLED'}")
    print("=" * 70)
    
    results = {
        "master_run_id": master_run_id,
        "config": {
            "pipeline_name": config.pipeline_name,
            "num_records": config.num_records,
            "inject_issues": inject_issues,
        },
    }
    
    # =========================================================================
    # BRONZE LAYER
    # =========================================================================
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
    
    # =========================================================================
    # SILVER LAYER
    # =========================================================================
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
        print(f"\n✅ Silver complete: {silver_results['orders_count']} orders, "
              f"{silver_results.get('quarantine_count', 0)} quarantined")
    except Exception as e:
        results["silver"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Silver FAILED: {e}")
        if config.fail_pipeline_on_critical:
            raise
    
    # =========================================================================
    # GOLD LAYER
    # =========================================================================
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
        print(f"\n✅ Gold complete: {gold_results['daily_revenue_count']} daily revenue rows, "
              f"{gold_results['product_count']} products, "
              f"{gold_results['customer_count']} customers")
    except Exception as e:
        results["gold"] = {"status": "FAILED", "error": str(e)}
        print(f"\n❌ Gold FAILED: {e}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    total_time = time.time() - pipeline_start
    results["total_execution_seconds"] = round(total_time, 2)
    
    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    
    for layer in ["bronze", "silver", "gold"]:
        layer_info = results.get(layer, {})
        status = layer_info.get("status", "SKIPPED")
        emoji = "✅" if status == "SUCCESS" else "❌"
        print(f"  {emoji} {layer.upper()}: {status}")
    
    print(f"\n  Total execution time: {total_time:.1f} seconds")
    print("=" * 70)
    
    return results


def run_sql_observability_demo(spark: SparkSession, config: PipelineConfig = None):
    """
    Run SQL-based observability queries against the pipeline output.
    
    This demonstrates dbt-style testing philosophy using SparkSQL.
    """
    if config is None:
        config = PipelineConfig.default()
    
    sql = SQLObservabilityQueries(spark)
    
    print("\n" + "=" * 70)
    print("📋 SQL OBSERVABILITY QUERIES (dbt-style)")
    print("=" * 70)
    
    # Register Silver tables as temp views
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
    
    # Run SQL checks
    print("\n--- Uniqueness Check ---")
    uniqueness_query = sql.uniqueness_sql("silver_orders", ["order_id"])
    print(uniqueness_query)
    result = spark.sql(uniqueness_query)
    result.show(5)
    
    print("\n--- Not Null Check ---")
    null_query = sql.not_null_sql("silver_orders", "customer_id")
    print(null_query)
    result = spark.sql(null_query)
    result.show()
    
    print("\n--- Accepted Values Check ---")
    av_query = sql.accepted_values_sql(
        "silver_orders", "order_status",
        ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned", "refunded"]
    )
    print(av_query)
    result = spark.sql(av_query)
    result.show(5)
    
    print("\n--- Freshness Check ---")
    fresh_query = sql.freshness_sql("silver_orders", "ingestion_timestamp", 4.0)
    print(fresh_query)
    result = spark.sql(fresh_query)
    result.show()


# Entry point for direct execution (e.g., triggered by Azure Data Factory)
if __name__ == "__main__":
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="Lakehouse Data Quality Pipeline")
    parser.add_argument("--env", type=str, default="local", help="Execution environment (local or azure)")
    parser.add_argument("--storage-account", type=str, default=None, help="ADLS Gen2 Storage Account name")
    parser.add_argument("--container", type=str, default="lakehouse", help="ADLS Gen2 Container name")
    parser.add_argument("--tenant-id", type=str, default=None, help="Azure Tenant ID")
    parser.add_argument("--pipeline-date", type=str, default=None, help="Pipeline execution date (simulating ADF parameter)")
    parser.add_argument("--dry-run", action="store_true", help="Initialize config and Spark session, but skip execution")
    
    args = parser.parse_args()
    
    print(f"🚀 Initializing pipeline for environment: {args.env.upper()}")
    if args.pipeline_date:
        print(f"📅 ADF Pipeline Date Parameter: {args.pipeline_date}")
    
    config = PipelineConfig()
    config.environment = args.env
    
    if args.env == "azure":
        # Simulate ADF passing parameters or pulling from environment variables
        config.azure_storage_account = args.storage_account or os.environ.get("AZURE_STORAGE_ACCOUNT", "lakehousedqprd")
        config.azure_container = args.container
        config.azure_tenant_id = args.tenant_id or os.environ.get("AZURE_TENANT_ID", "mock-tenant-id")
        config.paths.set_azure_config(config.azure_storage_account, config.azure_container)
        print(f"☁️ Configured for Azure Data Lake Storage Gen2: {config.paths.base_path}")
    else:
        print("💻 Configured for Local Execution")

    try:
        # Pass config to get_spark_session to trigger Azure AD Service Principal Auth if needed
        spark = get_spark_session("LakehouseDQ_Main", config=config)
        
        if args.dry_run:
            print("✅ Dry run complete. Configuration and Spark Session initialized successfully.")
        else:
            # Run full pipeline
            results = run_full_pipeline(config, inject_issues=False, spark=spark)
            
            # Run SQL observability demo
            run_sql_observability_demo(spark, config)
    
    except Exception as e:
        print(f"❌ Pipeline Failed: {e}")
        import traceback
        traceback.print_exc()
        # In an ADF context, a non-zero exit code marks the activity as Failed
        import sys
        sys.exit(1)
        
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()

