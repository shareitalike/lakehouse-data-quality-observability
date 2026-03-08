"""
Utility module for environment detection and path resolution.

This module centralizes storage path logic and SparkSession configuration,
providing environment-aware path resolution for Databricks and local 
development environments.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


def _is_databricks() -> bool:
    """
    Detect if running inside Databricks environment by checking for 
    DATABRICKS_RUNTIME_VERSION.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_base_path() -> str:
    """
    Determine base storage path based on environment.
    """
    force_local = os.environ.get("FORCE_LOCAL_PATHS", "").lower() == "true"
    
    if _is_databricks() and not force_local:
        # UPDATED FOR SERVERLESS: Use Unity Catalog Volume instead of DBFS
        # DBFS paths like /tmp/ and /user/ don't work on serverless compute
        return "/Volumes/dev/default/lakehouse_dq"
    else:
        # Local development — use project-relative path
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(project_root, "delta_output")


@dataclass
class LakehousePaths:
    """
    Centralized path configuration for all Delta tables in the framework.
    """
    base_path: str = field(default_factory=_get_base_path)
    
    def set_azure_config(self, storage_account: str, container: str):
        """
        Switches the base path to Azure Data Lake Storage Gen2 (ADLS Gen2) using abfss://
        """
        self.base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/lakehouse_dq"
    
    
    @property
    def bronze_raw(self) -> str:
        return os.path.join(self.base_path, "bronze", "raw_events")
    
    @property
    def bronze_quarantine(self) -> str:
        return os.path.join(self.base_path, "bronze", "quarantine")
    
    @property
    def silver_orders(self) -> str:
        return os.path.join(self.base_path, "silver", "orders")
    
    @property
    def silver_customers(self) -> str:
        return os.path.join(self.base_path, "silver", "customers")
    
    @property
    def silver_products(self) -> str:
        return os.path.join(self.base_path, "silver", "products")
    
    @property
    def silver_quarantine(self) -> str:
        return os.path.join(self.base_path, "silver", "quarantine")
    
    @property
    def gold_daily_revenue(self) -> str:
        return os.path.join(self.base_path, "gold", "daily_revenue")
    
    @property
    def gold_product_performance(self) -> str:
        return os.path.join(self.base_path, "gold", "product_performance")
    
    @property
    def gold_customer_ltv(self) -> str:
        return os.path.join(self.base_path, "gold", "customer_lifetime_value")
    
    @property
    def gold_quarantine(self) -> str:
        return os.path.join(self.base_path, "gold", "quarantine")
    
    @property
    def observability_metrics(self) -> str:
        return os.path.join(self.base_path, "observability", "metrics")
    
    @property
    def observability_rule_results(self) -> str:
        return os.path.join(self.base_path, "observability", "rule_results")
    
    def get_layer_paths(self, layer: str) -> dict:
        """Return all paths for a given Medallion layer."""
        layer_map = {
            "bronze": {
                "raw": self.bronze_raw,
                "quarantine": self.bronze_quarantine,
            },
            "silver": {
                "orders": self.silver_orders,
                "customers": self.silver_customers,
                "products": self.silver_products,
                "quarantine": self.silver_quarantine,
            },
            "gold": {
                "daily_revenue": self.gold_daily_revenue,
                "product_performance": self.gold_product_performance,
                "customer_ltv": self.gold_customer_ltv,
                "quarantine": self.gold_quarantine,
            },
        }
        if layer not in layer_map:
            raise ValueError(f"Unknown layer: {layer}. Must be bronze/silver/gold.")
        return layer_map[layer]


# Module-level singleton for convenience.
PATHS = LakehousePaths()


def get_spark_session(app_name: str = "LakehouseDQ", config=None):
    """
    Create or get SparkSession with Delta Lake support and optional Azure Auth.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    if _is_databricks():
        # On Databricks, session is pre-configured with Delta
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    else:
        try:
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                .config("spark.sql.warehouse.dir", os.path.join(PATHS.base_path, "warehouse"))
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.adaptive.enabled", "true")
            )
            
            # If we are mocking Azure locally without Databricks, we would need to 
            # add hadoop-azure packages here, but for this portfolio we focus on 
            # Databricks execution for Azure.
            
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception as e:
            if "ClassNotFoundException" in str(e) or "delta" in str(e).lower():
                raise RuntimeError(
                    "Delta Lake not found. Install with: "
                    "pip install delta-spark\n"
                    "Original error: " + str(e)
                )
            raise

    # Configure Azure ADLS Gen2 Authentication if Azure config is provided
    if config and getattr(config, "environment", "local") == "azure" and config.azure_storage_account:
        print(f"🔒 Configuring Spark Session for ADLS Gen2 OAuth on {config.azure_storage_account}")
        
        # In a real Databricks environment, we fetch from Secret Scope
        # dbutils = ... (fetch dbutils)
        # client_id = dbutils.secrets.get(scope=config.azure_client_id_secret_scope, key=config.azure_client_id_secret_key)
        
        # Here we mock the configuration for the consultant narrative:
        account_name = config.azure_storage_account
        spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        # In production, we would inject the real secrets here:
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", "mock-client-id")
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", "mock-client-secret")
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{config.azure_tenant_id}/oauth2/token")

    return spark

