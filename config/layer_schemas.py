"""
Layer schema configuration — data contracts per Medallion layer.

This module defines formal data contracts for each layer of the Medallion 
architecture, specifying structural schemas along with quality and SLA 
expectations to ensure data reliability and consistency.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pyspark.sql.types import StructType

from schemas.bronze_schemas import BRONZE_RAW_EVENTS_SCHEMA
from schemas.silver_schemas import (
    SILVER_ORDERS_SCHEMA,
    SILVER_CUSTOMERS_SCHEMA,
    SILVER_PRODUCTS_SCHEMA,
)
from schemas.gold_schemas import (
    GOLD_DAILY_REVENUE_SCHEMA,
    GOLD_PRODUCT_PERFORMANCE_SCHEMA,
    GOLD_CUSTOMER_LTV_SCHEMA,
)


@dataclass
class DataContract:
    """
    A data contract defines expectations for a dataset, combining structural 
    schemas with quality constraints.
    """
    dataset_name: str
    layer: str
    schema: StructType
    primary_key_columns: List[str]
    
    # Schema contract
    enforce_schema: bool = True
    # If True, reject data that doesn't match schema (Bronze mode)
    # If False, allow schema evolution (Silver/Gold mode)
    
    allow_schema_evolution: bool = False
    # Can the schema grow over time?
    
    # Quality contract
    min_not_null_rate: Dict[str, float] = field(default_factory=dict)
    # Column → minimum non-null rate (e.g., {"customer_id": 0.95})
    
    max_duplicate_rate: float = 0.0
    # Maximum acceptable duplicate rate on primary key
    
    # SLA contract
    max_freshness_hours: float = 24.0
    min_row_count: int = 0
    max_row_count: Optional[int] = None
    
    version: str = "1.0"


# =============================================================================
# PRE-BUILT CONTRACTS PER LAYER
# =============================================================================

BRONZE_RAW_EVENTS_CONTRACT = DataContract(
    dataset_name="bronze_raw_events",
    layer="bronze",
    schema=BRONZE_RAW_EVENTS_SCHEMA,
    primary_key_columns=["order_id"],
    enforce_schema=True,
    allow_schema_evolution=False,
    min_not_null_rate={"order_id": 1.0, "customer_id": 0.90},
    max_duplicate_rate=0.05,  # Allow up to 5% dupes in raw events
    max_freshness_hours=24.0,
    min_row_count=100,
    version="1.0",
)

SILVER_ORDERS_CONTRACT = DataContract(
    dataset_name="silver_orders",
    layer="silver",
    schema=SILVER_ORDERS_SCHEMA,
    primary_key_columns=["order_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"order_id": 1.0, "customer_id": 1.0, "product_id": 1.0},
    max_duplicate_rate=0.0,  # Zero duplicates after deduplication
    max_freshness_hours=4.0,
    min_row_count=50,
    version="1.0",
)

SILVER_CUSTOMERS_CONTRACT = DataContract(
    dataset_name="silver_customers",
    layer="silver",
    schema=SILVER_CUSTOMERS_SCHEMA,
    primary_key_columns=["customer_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"customer_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=4.0,
    version="1.0",
)

SILVER_PRODUCTS_CONTRACT = DataContract(
    dataset_name="silver_products",
    layer="silver",
    schema=SILVER_PRODUCTS_SCHEMA,
    primary_key_columns=["product_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"product_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=4.0,
    version="1.0",
)

GOLD_DAILY_REVENUE_CONTRACT = DataContract(
    dataset_name="gold_daily_revenue",
    layer="gold",
    schema=GOLD_DAILY_REVENUE_SCHEMA,
    primary_key_columns=["order_date", "currency"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"order_date": 1.0, "total_revenue": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

GOLD_PRODUCT_PERFORMANCE_CONTRACT = DataContract(
    dataset_name="gold_product_performance",
    layer="gold",
    schema=GOLD_PRODUCT_PERFORMANCE_SCHEMA,
    primary_key_columns=["product_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"product_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

GOLD_CUSTOMER_LTV_CONTRACT = DataContract(
    dataset_name="gold_customer_ltv",
    layer="gold",
    schema=GOLD_CUSTOMER_LTV_SCHEMA,
    primary_key_columns=["customer_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"customer_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

# Registry of all contracts for easy lookup
ALL_CONTRACTS = {
    "bronze_raw_events": BRONZE_RAW_EVENTS_CONTRACT,
    "silver_orders": SILVER_ORDERS_CONTRACT,
    "silver_customers": SILVER_CUSTOMERS_CONTRACT,
    "silver_products": SILVER_PRODUCTS_CONTRACT,
    "gold_daily_revenue": GOLD_DAILY_REVENUE_CONTRACT,
    "gold_product_performance": GOLD_PRODUCT_PERFORMANCE_CONTRACT,
    "gold_customer_ltv": GOLD_CUSTOMER_LTV_CONTRACT,
}
