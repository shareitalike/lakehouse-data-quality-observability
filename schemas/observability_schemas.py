"""
Observability layer schema definitions.



# FAILURE MODE: If the observability table itself becomes corrupted or
# accidentally truncated, you lose ALL historical trend data. Mitigation:
# (1) append-only mode prevents accidental overwrites, (2) Delta time travel
# allows recovery, (3) periodic backups of the Delta table.

# NOTE: Why is your observability table append-only?"
# → "Three reasons: immutable audit trail for compliance, time-series trend
#    analysis requires historical data, and append-only prevents accidental
#    deletion of critical monitoring data. You can't do trend-based alerting
#    if someone accidentally runs a DELETE."

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
    MapType,
)


# =============================================================================
# OBSERVABILITY METRICS TABLE
# =============================================================================
# This is the core metrics store — every validation run writes here.

OBSERVABILITY_METRICS_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    
    StructField("dataset_name", StringType(), nullable=False),
    # Which dataset was validated (e.g., "bronze_raw_events", "silver_orders")
    
    StructField("layer", StringType(), nullable=False),
    # Medallion layer: "bronze", "silver", "gold"
    
    StructField("rule_name", StringType(), nullable=False),
    # Name of the validation rule (e.g., "not_null_check")
    
    StructField("rule_version", StringType(), nullable=False),
    # Version of the rule config used. Critical for audit: "which version
    # of the rule was active when this metric was captured?"
    # NOTE: Why version your validation rules?"
    # → "Because rule thresholds change over time. When investigating a past
    #    incident, you need to know what thresholds were active. Without
    #    versioning, you can't distinguish 'rule was too loose' from
    #    'data was genuinely bad.'"
    
    StructField("metric_name", StringType(), nullable=False),
    # Specific metric: "null_rate", "duplicate_rate", "freshness_lag_seconds",
    # "row_count", "pass_rate", "distribution_drift_score", etc.
    
    StructField("metric_value", DoubleType(), nullable=False),
    # Numeric value of the metric
    
    StructField("passed", BooleanType(), nullable=False),
    # Whether the rule passed or failed for this run
    
    StructField("severity", StringType(), nullable=False),
    # "critical", "warning", "info"
    
    StructField("total_records", LongType(), nullable=False),
    # Total records evaluated
    
    StructField("failed_records", LongType(), nullable=False),
    # Records that failed this rule
    
    StructField("run_timestamp", TimestampType(), nullable=False),
    # When this validation run occurred
    
    StructField("pipeline_name", StringType(), nullable=True),
    # Optional: which pipeline triggered this run
    
    StructField("metadata_json", StringType(), nullable=True),
])


# =============================================================================
# OBSERVABILITY RULE RESULTS TABLE
# =============================================================================
# Detailed per-record results for debugging. Higher cardinality than metrics.

OBSERVABILITY_RULE_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("rule_name", StringType(), nullable=False),
    StructField("dataset_name", StringType(), nullable=False),
    StructField("record_id", StringType(), nullable=True),
    # Primary key of the failed record for traceability
    
    StructField("failure_reason", StringType(), nullable=True),
    # Human-readable explanation: "customer_id is null", "duplicate order_id"
    
    StructField("record_snapshot", StringType(), nullable=True),
    # JSON serialization of the failed record for debugging.
    
    StructField("run_timestamp", TimestampType(), nullable=False),
])


# Partition strategy for observability tables
OBSERVABILITY_PARTITION_COLUMNS = ["run_timestamp"]
