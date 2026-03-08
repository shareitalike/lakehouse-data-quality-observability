"""
Bronze layer schema definitions.

This module provides explicit StructType schemas for the Bronze layer, ensuring 
consistent data ingestion and facilitating early-stage schema drift detection.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
)


# =============================================================================
# BRONZE RAW EVENTS SCHEMA
# =============================================================================
# This is the "expected" schema for incoming raw order events.
# The schema drift detector compares actual data against this baseline.

BRONZE_RAW_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    
    StructField("product_id", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("order_timestamp", StringType(), nullable=True),
])


# Valid order status values (used by accepted_values_check)
VALID_ORDER_STATUSES = [
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]

# Valid currencies (used by accepted_values_check)
VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD"]

# Expected primary key columns at Bronze
BRONZE_PRIMARY_KEY = ["order_id"]

# Columns that must not be null (critical fields)
BRONZE_CRITICAL_NOT_NULL_COLUMNS = ["order_id"]

# Columns that should not be null (warning level)
BRONZE_WARNING_NOT_NULL_COLUMNS = ["customer_id", "product_id", "order_timestamp"]
