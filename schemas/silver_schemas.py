"""
Silver layer schema definitions.

This module defines strict, typed schemas for the Silver layer datasets, 
representing cleaned and normalized data ready for business logic application 
and aggregation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
    DateType,
)


# =============================================================================
# SILVER ORDERS SCHEMA
# =============================================================================

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    
    StructField("product_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    
    StructField("currency", StringType(), nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False),
    
    StructField("order_date", DateType(), nullable=False),
    
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    
    StructField("source_file", StringType(), nullable=True),
    # Lineage tracking — which Bronze batch this record came from.
])

SILVER_ORDERS_PRIMARY_KEY = ["order_id"]


# =============================================================================
# SILVER CUSTOMERS SCHEMA
# =============================================================================

SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_order_date", DateType(), nullable=True),
    StructField("last_order_date", DateType(), nullable=True),
    StructField("total_orders", LongType(), nullable=False),
    StructField("total_spend", DoubleType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("preferred_currency", StringType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

SILVER_CUSTOMERS_PRIMARY_KEY = ["customer_id"]


# =============================================================================
# SILVER PRODUCTS SCHEMA
# =============================================================================

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("total_quantity_sold", LongType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("avg_unit_price", DoubleType(), nullable=True),
    StructField("min_unit_price", DoubleType(), nullable=True),
    StructField("max_unit_price", DoubleType(), nullable=True),
    StructField("order_count", LongType(), nullable=False),
    StructField("last_sold_date", DateType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

SILVER_PRODUCTS_PRIMARY_KEY = ["product_id"]
