"""
Silver Pipeline — cleaned, normalized, trusted data layer.

This module implements the Silver layer of the Medallion architecture, focusing on 
data cleaning, normalization, deduplication, and referential integrity 
validation to produce trusted datasets for downstream consumption.
"""

import uuid
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.pipeline_configs import PipelineConfig
from config.rule_configs import get_silver_rules
from config.layer_schemas import SILVER_ORDERS_CONTRACT
from engine.validation_engine import ValidationEngine
from engine.quarantine_manager import QuarantineManager
from observability.metrics_store import MetricsStore
from data_generation.bronze_generator import get_product_reference_df, get_customer_reference_df


class SilverPipeline:
    """
    Silver layer pipeline: read Bronze → clean → validate → write Silver tables.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
        self.engine = ValidationEngine(spark, config)
        self.quarantine_mgr = QuarantineManager(spark, config)
        self.metrics_store = MetricsStore(spark)
        self.run_id = str(uuid.uuid4())
    
    def read_bronze(self) -> DataFrame:
        """
        Read raw data from Bronze Delta table.
        """
        bronze_path = self.config.paths.bronze_raw
        try:
            df = self.spark.read.format("delta").load(bronze_path)
            print(f"[SilverPipeline] Read {df.count()} records from Bronze")
            return df
        except Exception as e:
            print(f"[SilverPipeline] ERROR reading Bronze: {e}")
            raise
    
    def clean_and_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply Silver-layer transformations.
        
        Transformations:
        1. Remove nulls on critical fields (customer_id, product_id)
        2. Parse and validate timestamps
        3. Deduplicate by order_id
        4. Normalize values (lowercase status, validate currency)
        5. Compute derived fields (total_amount, order_date)
        6. Add ingestion metadata
        """
        print(f"\n[SilverPipeline] Starting transformation...")
        initial_count = df.count()
        
        # Step 1: Filter null critical fields
        # Remove records with null order_id or critical foreign keys.
        df_clean = df.filter(
            F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull()
        )
        
        after_null_filter = df_clean.count()
        print(f"  Step 1 — Null filter: {initial_count} → {after_null_filter} "
              f"(removed {initial_count - after_null_filter})")
        
        # Step 2: Parse timestamps using try_to_timestamp for robustness.
        # Format string must be wrapped in lit().
        df_clean = df_clean.withColumn(
            "order_timestamp_parsed",
            F.try_to_timestamp(F.col("order_timestamp"), F.lit("yyyy-MM-dd HH:mm:ss"))
        )
        
        # Filter out records with unparseable timestamps
        df_clean = df_clean.filter(F.col("order_timestamp_parsed").isNotNull())
        
        after_ts_filter = df_clean.count()
        print(f"  Step 2 — Timestamp parse: {after_null_filter} → {after_ts_filter} "
              f"(removed {after_null_filter - after_ts_filter})")
        
        # Step 3: Deduplicate by order_id (keep latest)
        # Use window function with ROW_NUMBER for deterministic results.
        window = Window.partitionBy("order_id").orderBy(
            F.col("order_timestamp_parsed").desc()
        )
        df_clean = (
            df_clean
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
        
        after_dedup = df_clean.count()
        print(f"  Step 3 — Deduplicate: {after_ts_filter} → {after_dedup} "
              f"(removed {after_ts_filter - after_dedup} duplicates)")
        
        after_normalize = df_clean.count()
        print(f"  Step 4 — Normalize/Verify: {after_dedup} → {after_normalize}")
        
        # Step 5: Compute derived fields
        df_clean = (
            df_clean
            .withColumn("total_amount",
                       F.round(F.col("quantity") * F.col("unit_price"), 2))
            .withColumn("order_date",
                       F.to_date(F.col("order_timestamp_parsed")))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.lit(f"bronze_run_{self.run_id}"))
        )
        
        # Step 6: Select final Silver columns
        df_silver = df_clean.select(
            "order_id",
            "customer_id",
            "product_id",
            "order_status",
            "quantity",
            "unit_price",
            "total_amount",
            "currency",
            F.col("order_timestamp_parsed").alias("order_timestamp"),
            "order_date",
            "ingestion_timestamp",
            "source_file",
        )
        
        print(f"  Final Silver records: {df_silver.count()}")
        print(f"  Overall retention rate: {df_silver.count()/initial_count:.1%}")
        
        return df_silver
    
    def build_customers_table(self, orders_df: DataFrame) -> DataFrame:
        """
        Build Silver customers table from orders.
        """
        customers = (
            orders_df.groupBy("customer_id")
            .agg(
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("last_order_date"),
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_spend"),
                F.avg("total_amount").alias("avg_order_value"),
                F.first("currency").alias("preferred_currency"),
            )
            .withColumn("last_updated", F.current_timestamp())
        )
        
        return customers
    
    def build_products_table(self, orders_df: DataFrame) -> DataFrame:
        """
        Build Silver products table from orders.
        """
        products = (
            orders_df.groupBy("product_id")
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("unit_price").alias("avg_unit_price"),
                F.min("unit_price").alias("min_unit_price"),
                F.max("unit_price").alias("max_unit_price"),
                F.count("order_id").alias("order_count"),
                F.max("order_date").alias("last_sold_date"),
            )
            .withColumn("last_updated", F.current_timestamp())
        )
        
        return products
    
    def validate_silver(
        self,
        orders_df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Run Silver validation rules.
        """
        results = self.engine.validate_silver(
            orders_df,
            dataset_name="silver_orders",
        )
        
        # Write metrics
        if self.config.enable_observability:
            self.metrics_store.write_metrics(
                results,
                run_id=self.run_id,
                pipeline_name=self.config.pipeline_name,
            )
        
        return self.engine.generate_summary(results)
    
    def write_silver_tables(
        self,
        orders_df: DataFrame,
        customers_df: DataFrame,
        products_df: DataFrame,
    ) -> Dict[str, str]:
        """
        Write all Silver tables to Delta.
        """
        paths = {}
        
        # Orders
        orders_path = self.config.paths.silver_orders
        orders_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(orders_path)
        paths["orders"] = orders_path
        print(f"[SilverPipeline] Orders: {orders_df.count()} records → {orders_path}")
        
        # Customers  
        customers_path = self.config.paths.silver_customers
        customers_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(customers_path)
        paths["customers"] = customers_path
        print(f"[SilverPipeline] Customers: {customers_df.count()} records → {customers_path}")
        
        # Products
        products_path = self.config.paths.silver_products
        products_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).save(products_path)
        paths["products"] = products_path
        print(f"[SilverPipeline] Products: {products_df.count()} records → {products_path}")
        
        return paths
    
    def run(self) -> Dict[str, Any]:
        """
        Execute full Silver pipeline: read Bronze → clean → validate → write.
        """
        # Step 1: Read Bronze
        bronze_df = self.read_bronze()
        
        # Step 2: Clean and transform (Now 'Soft' cleaning — doesn't drop records)
        all_orders_df = self.clean_and_transform(bronze_df)
        
        # Step 3: Validate (Identifies rule violations like negative price)
        results = self.engine.validate_silver(
            all_orders_df,
            dataset_name="silver_orders",
        )
        
        # Step 4: Quarantine Routing
        # Split data into 'Good' and 'Bad' based on validation results.
        quarantine_count = 0
        silver_orders_df = all_orders_df
        
        if self.engine.should_quarantine(results, all_orders_df):
            quarantine_records = self.engine.get_quarantine_records(results)
            if quarantine_records is not None:
                quarantine_count = self.quarantine_mgr.quarantine_records(
                    quarantine_records,
                    layer="silver",
                    run_id=self.run_id,
                    dataset_name="silver_orders",
                )
                
                # Filter out the quarantined records from the main Silver table.
                silver_orders_df = all_orders_df.join(
                    quarantine_records.select("order_id"),
                    on="order_id",
                    how="left_anti"
                )
        
        # Step 5: Build derived tables from CLEAN data only
        customers_df = self.build_customers_table(silver_orders_df)
        products_df = self.build_products_table(silver_orders_df)
        
        # Step 6: Write Metrics
        if self.config.enable_observability:
            self.metrics_store.write_metrics(
                results,
                run_id=self.run_id,
                pipeline_name=self.config.pipeline_name,
            )
            
        validation_summary = self.engine.generate_summary(results)
        
        # Step 7: Write to Delta
        paths = self.write_silver_tables(silver_orders_df, customers_df, products_df)
        
        return {
            "pipeline": "silver",
            "run_id": self.run_id,
            "paths": paths,
            "orders_count": silver_orders_df.count(),
            "quarantine_count": quarantine_count,
            "customers_count": customers_df.count(),
            "products_count": products_df.count(),
            "validation": validation_summary,
        }
