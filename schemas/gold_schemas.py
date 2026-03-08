"""
Gold layer schema definitions.

This module provides schema definitions for business-facing aggregated Gold 
tables, optimized for reporting, analytics, and final data quality validation.
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
# GOLD: DAILY REVENUE
# =============================================================================

GOLD_DAILY_REVENUE_SCHEMA = StructType([
    StructField("order_date", DateType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("order_count", LongType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("unique_customers", LongType(), nullable=False),
    StructField("unique_products", LongType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    
    StructField("computed_at", TimestampType(), nullable=False),
    # When this aggregate was computed — for freshness SLA checks.
])

GOLD_DAILY_REVENUE_PRIMARY_KEY = ["order_date", "currency"]


# =============================================================================
# GOLD: PRODUCT PERFORMANCE
# =============================================================================

GOLD_PRODUCT_PERFORMANCE_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("total_quantity_sold", LongType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("avg_unit_price", DoubleType(), nullable=True),
    StructField("order_count", LongType(), nullable=False),
    StructField("unique_customers", LongType(), nullable=False),
    StructField("first_sold_date", DateType(), nullable=True),
    StructField("last_sold_date", DateType(), nullable=True),
    StructField("revenue_rank", IntegerType(), nullable=True),
    
    StructField("computed_at", TimestampType(), nullable=False),
])

GOLD_PRODUCT_PERFORMANCE_PRIMARY_KEY = ["product_id"]


# =============================================================================
# GOLD: CUSTOMER LIFETIME VALUE
# =============================================================================

GOLD_CUSTOMER_LTV_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("total_orders", LongType(), nullable=False),
    StructField("total_spend", DoubleType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("first_order_date", DateType(), nullable=True),
    StructField("last_order_date", DateType(), nullable=True),
    StructField("customer_tenure_days", IntegerType(), nullable=True),
    StructField("orders_per_month", DoubleType(), nullable=True),
    
    StructField("ltv_segment", StringType(), nullable=True),
    
    StructField("computed_at", TimestampType(), nullable=False),
])

GOLD_CUSTOMER_LTV_PRIMARY_KEY = ["customer_id"]
