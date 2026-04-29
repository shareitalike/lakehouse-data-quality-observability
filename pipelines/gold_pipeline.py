"""
Gold Pipeline — business-facing aggregated metrics.

This module implements the Gold layer of the Medallion architecture, focusing on 
business-facing aggregations and final data quality validation to provide 
high-quality insights for dashboards and reporting.
"""

import uuid
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config.pipeline_configs import PipelineConfig
from config.rule_configs import get_gold_rules
from engine.validation_engine import ValidationEngine
from observability.metrics_store import MetricsStore


class GoldPipeline:
    """
    Gold layer pipeline: read Silver → aggregate → validate → write.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
        self.engine = ValidationEngine(spark, config)
        self.metrics_store = MetricsStore(spark)
        self.run_id = str(uuid.uuid4())
    
    def read_silver(self) -> Dict[str, DataFrame]:
        """Read all Silver tables."""
        paths = self.config.paths
        
        tables = {}
        try:
            tables["orders"] = self.spark.read.format("delta").load(paths.silver_orders)
            print(f"[GoldPipeline] Read {tables['orders'].count()} orders from Silver")
        except Exception as e:
            print(f"[GoldPipeline] ERROR reading Silver orders: {e}")
            raise
        
        try:
            tables["customers"] = self.spark.read.format("delta").load(paths.silver_customers)
            tables["products"] = self.spark.read.format("delta").load(paths.silver_products)
        except Exception as e:
            print(f"[GoldPipeline] WARNING: Could not read customer/product Silver tables: {e}")
        
        return tables
    
    def build_daily_revenue(self, orders_df: DataFrame) -> DataFrame:
        """
        Build daily revenue aggregation.
        """
        daily_rev = (
            orders_df
            .groupBy("order_date", "currency")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.count("order_id").alias("order_count"),
                F.avg("total_amount").alias("avg_order_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("product_id").alias("unique_products"),
            )
            .withColumn("computed_at", F.current_timestamp())
        )
        
        return daily_rev
    
    def build_product_performance(self, orders_df: DataFrame) -> DataFrame:
        """
        Build product performance metrics.
        """
        product_perf = (
            orders_df
            .groupBy("product_id")
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("unit_price").alias("avg_unit_price"),
                F.count("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.min("order_date").alias("first_sold_date"),
                F.max("order_date").alias("last_sold_date"),
            )
        )
        
        # Add product name placeholder
        product_perf = product_perf.withColumn("product_name", F.lit(None).cast("string"))
        
        # Add revenue rank
        rank_window = Window.orderBy(F.desc("total_revenue"))
        product_perf = (
            product_perf
            .withColumn("revenue_rank", F.row_number().over(rank_window))
            .withColumn("computed_at", F.current_timestamp())
        )
        
        return product_perf
    
    def build_customer_ltv(self, orders_df: DataFrame) -> DataFrame:
        """
        Build customer lifetime value metrics.
        """
        customer_ltv = (
            orders_df
            .groupBy("customer_id")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_spend"),
                F.avg("total_amount").alias("avg_order_value"),
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("last_order_date"),
            )
        )
        
        # Compute tenure days
        customer_ltv = customer_ltv.withColumn(
            "customer_tenure_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date"))
        )
        
        # Step 5: Compute tenure days and orders per month.
        customer_ltv = customer_ltv.withColumn(
            "orders_per_month",
            F.when(
                F.col("customer_tenure_days") > 0,
                F.round(
                    F.col("total_orders") / (F.col("customer_tenure_days") / 30.0),
                    2
                )
            ).otherwise(F.col("total_orders").cast("double"))
        )
        
        # Compute LTV segment (High, Medium, Low).
        customer_ltv = customer_ltv.withColumn(
            "ltv_segment",
            F.when(
                F.col("total_spend") >= F.lit(500.0), F.lit("high")
            ).when(
                F.col("total_spend") >= F.lit(100.0), F.lit("medium")
            ).otherwise(F.lit("low"))
        )
        
        customer_ltv = customer_ltv.withColumn("computed_at", F.current_timestamp())
        
        return customer_ltv
    
    def validate_gold(
        self,
        daily_revenue_df: DataFrame,
        product_perf_df: DataFrame,
        customer_ltv_df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Run Gold validation rules across all Gold tables.
        """
        # Validate daily revenue (most important Gold table)
        results = self.engine.validate_gold(
            daily_revenue_df,
            dataset_name="gold_daily_revenue",
        )
        
        # Write metrics
        if self.config.enable_observability:
            self.metrics_store.write_metrics(
                results,
                run_id=self.run_id,
                pipeline_name=self.config.pipeline_name,
            )
        
        return self.engine.generate_summary(results)
    
    def write_gold_tables(
        self,
        daily_revenue_df: DataFrame,
        product_perf_df: DataFrame,
        customer_ltv_df: DataFrame,
    ) -> Dict[str, str]:
        """
        Write Gold tables to Delta using MERGE (UPSERT) for Idempotency.
        """
        paths = {}
        
        # Helper to merge or create
        def merge_or_create(df: DataFrame, path: str, merge_condition: str):
            if DeltaTable.isDeltaTable(self.spark, path):
                dt = DeltaTable.forPath(self.spark, path)
                dt.alias("target").merge(
                    df.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                print(f"[GoldPipeline] Merged {df.count()} records into {path}")
            else:
                df.write.format("delta").mode("overwrite").save(path)
                print(f"[GoldPipeline] Created new Delta table at {path} with {df.count()} records")
        
        # Daily Revenue (Merge on date and currency)
        rev_path = self.config.paths.gold_daily_revenue
        merge_or_create(daily_revenue_df, rev_path, "target.order_date = source.order_date AND target.currency = source.currency")
        paths["daily_revenue"] = rev_path
        
        # Product Performance (Merge on product_id)
        prod_path = self.config.paths.gold_product_performance
        merge_or_create(product_perf_df, prod_path, "target.product_id = source.product_id")
        paths["product_performance"] = prod_path
        
        # Customer LTV (Merge on customer_id)
        ltv_path = self.config.paths.gold_customer_ltv
        merge_or_create(customer_ltv_df, ltv_path, "target.customer_id = source.customer_id")
        paths["customer_ltv"] = ltv_path
        
        return paths
    
    def run(self) -> Dict[str, Any]:
        """
        Execute full Gold pipeline: read Silver → aggregate → validate → write.
        """
        # Step 1: Read Silver
        silver_tables = self.read_silver()
        orders_df = silver_tables["orders"]
        
        # Step 2: Build aggregations
        daily_revenue = self.build_daily_revenue(orders_df)
        product_perf = self.build_product_performance(orders_df)
        customer_ltv = self.build_customer_ltv(orders_df)
        
        # Step 3: Validate
        validation = self.validate_gold(daily_revenue, product_perf, customer_ltv)
        
        # Step 4: Write Gold tables
        paths = self.write_gold_tables(daily_revenue, product_perf, customer_ltv)
        
        return {
            "pipeline": "gold",
            "run_id": self.run_id,
            "paths": paths,
            "daily_revenue_count": daily_revenue.count(),
            "product_count": product_perf.count(),
            "customer_count": customer_ltv.count(),
            "validation": validation,
        }
