"""
Rule configuration dataclasses.

This module defines the configuration structures for all validation rules 
within the framework, providing a type-safe and extensible way to manage 
data quality expectations.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum


# =============================================================================
# ENUMS FOR TYPE SAFETY
# =============================================================================

class Severity(str, Enum):
    """
    Rule severity levels.
    
    
    # INTERVIEW: "What happens when a critical rule fails?"
    # → "The record is routed to quarantine. The pipeline continues.
    #    We never crash pipelines on bad data — that causes cascading
    #    failures in production."
    """
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Layer(str, Enum):
    """Medallion architecture layer."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


# =============================================================================
# BASE RULE CONFIG
# =============================================================================

@dataclass
class BaseRuleConfig:
    """
    Base configuration shared by all validation rules.
    
    
    """
    rule_id: str
    rule_version: str = "1.0"
    severity: Severity = Severity.WARNING
    layer: Layer = Layer.BRONZE
    enabled: bool = True
    description: str = ""
    owner: str = ""  # Team or person responsible for this rule
    
    def __post_init__(self):
        """
        Validate and normalize config values.
        
        """
        if not self.rule_id:
            raise ValueError("rule_id cannot be empty")
        if isinstance(self.severity, str):
            self.severity = Severity(self.severity.lower())
        if isinstance(self.layer, str):
            self.layer = Layer(self.layer.lower())


# =============================================================================
# SPECIFIC RULE CONFIGS
# =============================================================================

@dataclass
class NotNullRule(BaseRuleConfig):
    """
    Configuration for not-null validation.
    
    """
    column_name: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        if not self.column_name:
            raise ValueError(f"Rule {self.rule_id}: column_name is required")


@dataclass
class UniqueKeyRule(BaseRuleConfig):
    """
    Configuration for unique key validation.
    
    
    # FAILURE MODE: If the key columns are all nullable, unique check
    # passes for multiple records where all key columns are null.
    # This is because NULL != NULL in SQL/Spark.
    # Mitigation: Always pair uniqueness checks with not-null checks.
    """
    columns: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        super().__post_init__()
        if not self.columns:
            raise ValueError(f"Rule {self.rule_id}: columns list cannot be empty")


@dataclass
class AcceptedValuesRule(BaseRuleConfig):
    """
    Configuration for accepted values validation (enum check).
    
    
    """
    column_name: str = ""
    accepted_values: List[str] = field(default_factory=list)
    case_sensitive: bool = False
    reference_table: Optional[str] = None
    # If set, join against this Delta table instead of using accepted_values list
    
    def __post_init__(self):
        super().__post_init__()
        if not self.column_name:
            raise ValueError(f"Rule {self.rule_id}: column_name is required")
        if not self.accepted_values and not self.reference_table:
            raise ValueError(
                f"Rule {self.rule_id}: either accepted_values or reference_table is required"
            )


@dataclass
class PositiveNumericRule(BaseRuleConfig):
    """
    Configuration for positive numeric validation.
    
    """
    column_name: str = ""
    allow_zero: bool = False
    
    def __post_init__(self):
        super().__post_init__()
        if not self.column_name:
            raise ValueError(f"Rule {self.rule_id}: column_name is required")


@dataclass
class TimestampFreshnessRule(BaseRuleConfig):
    """
    Configuration for timestamp freshness validation (SLA check).
    
    
    # FAILURE MODE: If the Spark cluster clock is skewed (common in cloud),
    # freshness checks produce false positives. Always use the cluster's
    # current_timestamp(), not Python datetime.now().
    
    # INTERVIEW: "How do you handle clock skew in freshness checks?"
    # → "Use Spark's current_timestamp() which is consistent across the
    #    cluster, not Python's datetime which varies per executor."
    """
    column_name: str = ""
    max_age_hours: float = 24.0
    
    def __post_init__(self):
        super().__post_init__()
        if not self.column_name:
            raise ValueError(f"Rule {self.rule_id}: column_name is required")
        if self.max_age_hours <= 0:
            raise ValueError(f"Rule {self.rule_id}: max_age_hours must be positive")


@dataclass
class DuplicateDetectionRule(BaseRuleConfig):
    """
    Configuration for duplicate detection.
    
    
    # INTERVIEW: "Why separate duplicate detection from unique key check?"
    # → "Unique key check tells you IF duplicates exist. Duplicate detection
    #    tells you WHICH record to keep. Different questions, different actions."
    """
    key_columns: List[str] = field(default_factory=list)
    order_by_column: str = ""
    # Tiebreaker: keep the record with the latest value of this column
    keep: str = "last"  # "first" or "last"
    
    def __post_init__(self):
        super().__post_init__()
        if not self.key_columns:
            raise ValueError(f"Rule {self.rule_id}: key_columns cannot be empty")


@dataclass
class SchemaDriftRule(BaseRuleConfig):
    """
    Configuration for schema drift detection.
    
    
    # INTERVIEW: "What's the difference between schema enforcement and
    # schema drift detection?"
    # → "Schema enforcement PREVENTS non-conforming data from being written.
    #    Schema drift detection ALERTS you that the upstream schema changed.
    #    Enforcement is a gate; drift detection is an alarm."
    """
    expected_columns: List[str] = field(default_factory=list)
    # List of expected column names
    
    allow_extra_columns: bool = False
    # If True, new columns are logged but not flagged as failures
    
    alert_on_missing: bool = True
    # Alert if expected columns are missing from the data
    
    alert_on_type_change: bool = True
    # Alert if column types have changed
    
    def __post_init__(self):
        super().__post_init__()
        if not self.expected_columns:
            raise ValueError(f"Rule {self.rule_id}: expected_columns cannot be empty")


@dataclass
class ReferentialIntegrityRule(BaseRuleConfig):
    """
    Configuration for referential integrity check.
    
    
    
    """
    child_column: str = ""
    # Column in the dataset being validated
    
    parent_table_path: str = ""
    # Path to the Delta table containing valid reference values
    
    parent_column: str = ""
    # Column in the reference table to match against
    
    def __post_init__(self):
        super().__post_init__()
        if not self.child_column:
            raise ValueError(f"Rule {self.rule_id}: child_column is required")
        if not self.parent_column:
            raise ValueError(f"Rule {self.rule_id}: parent_column is required")


@dataclass
class RowCountAnomalyRule(BaseRuleConfig):
    """
    Configuration for row count anomaly detection.
    
    
    # FAILURE MODE: First run has no historical data → no anomaly detection.
    # Mitigation: skip anomaly detection on first run, log baseline instead.
    
    # INTERVIEW: "Why percentage-based instead of absolute thresholds?"
    # → "A table that normally has 1M rows and drops to 500K is a 50% drop—
    #    clearly anomalous. But a table that normally has 100 rows and goes to
    #    50 might be normal weekend traffic. Percentages are context-aware."
    """
    min_deviation_pct: float = -30.0
    # Alert if row count drops more than this % below historical mean
    
    max_deviation_pct: float = 50.0
    # Alert if row count exceeds this % above historical mean
    
    lookback_runs: int = 10
    # Number of historical runs to compute the baseline mean
    
    def __post_init__(self):
        super().__post_init__()
        if self.min_deviation_pct > 0:
            raise ValueError(
                f"Rule {self.rule_id}: min_deviation_pct should be negative (e.g., -30.0)"
            )


@dataclass
class DistributionAnomalyRule(BaseRuleConfig):
    """
    Configuration for distribution anomaly detection.
    
    
    # WHY DISTRIBUTION DRIFT MATTERS:
    # 1. Price manipulation: a bot changes all prices to $0.01
    # 2. Data pipeline bug: currency conversion applied twice → 100x prices
    # 3. Feature store drift: ML model input distribution shifts → prediction
    #    quality degrades silently
    # 4. Seasonal change vs real anomaly: Black Friday doubles revenue (expected)
    #    vs a bug doubling revenue (unexpected) — distribution shape tells the
    #    difference.
    
    # WHY MEAN COMPARISON ALONE IS INSUFFICIENT:
    # Anscombe's quartet: four datasets with identical mean/variance but
    # completely different distributions. Mean tells you the "center" but
    # nothing about shape, spread pattern, or outlier behavior.
    # Example: mean revenue = $50. But is it 1000 orders at $50, or 1 order
    # at $50,000 + 999 orders at $0.05? Mean can't tell you.
    
    # Z-SCORE VS PERCENTILE DETECTION:
    # Z-score: (value - mean) / stddev. Parametric — assumes normal distribution.
    #   Good for: detecting single-point outliers in bell-curve data.
    #   Bad for: skewed distributions (log-normal prices), multi-modal data.
    # Percentile: compare P25/P50/P75 bands against baseline.
    #   Good for: distribution-agnostic, handles skew naturally.
    #   Bad for: misses tail changes if percentile granularity is too coarse.
    
    # INTERVIEW: "Why do you use both z-score and percentile detection?"
    # → "Z-score catches outlier injection (single bad values). Percentile
    #    catches distribution shift (entire shape changes). A price manipulation
    #    bot might not change the mean much but will destroy the percentile
    #    structure. Conversely, a single massive outlier shows in z-score
    #    but might not move percentiles."
    
    """
    column_name: str = ""
    z_score_threshold: float = 3.0
    # Standard deviations from historical mean to flag as anomalous
    
    percentile_bands: List[float] = field(default_factory=lambda: [0.25, 0.50, 0.75])
    # Percentiles to track for distribution shape comparison
    
    max_percentile_drift_pct: float = 20.0
    # Alert if any percentile shifts more than this % from baseline
    
    min_sample_size: int = 100
    # Minimum records needed for reliable statistics
    
    use_approx_quantile: bool = True
    
    relative_error: float = 0.01
    # Acceptable error for approximate quantiles (1% default)
    
    def __post_init__(self):
        super().__post_init__()
        if not self.column_name:
            raise ValueError(f"Rule {self.rule_id}: column_name is required")
        if self.z_score_threshold <= 0:
            raise ValueError(f"Rule {self.rule_id}: z_score_threshold must be positive")


# =============================================================================
# RULE SET: PREDEFINED CONFIGURATIONS PER LAYER
# =============================================================================

def get_bronze_rules() -> List[BaseRuleConfig]:
    """
    Default rule configurations for Bronze layer validation.
    
    """
    return [
        NotNullRule(
            rule_id="bronze_order_id_not_null",
            rule_version="1.0",
            column_name="order_id",
            severity=Severity.CRITICAL,
            layer=Layer.BRONZE,
            description="Order ID must never be null — it's the primary key",
        ),
        NotNullRule(
            rule_id="bronze_customer_id_not_null",
            rule_version="1.0",
            column_name="customer_id",
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Customer ID should not be null for attribution",
        ),
        UniqueKeyRule(
            rule_id="bronze_order_id_unique",
            rule_version="1.0",
            columns=["order_id"],
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Detect duplicate order_ids in raw events",
            # WARNING not CRITICAL: Bronze sees raw events which may have
            # legitimate retries. Deduplication happens in Silver.
        ),
        AcceptedValuesRule(
            rule_id="bronze_order_status_valid",
            rule_version="1.0",
            column_name="order_status",
            accepted_values=[
                "pending", "confirmed", "shipped",
                "delivered", "cancelled", "returned", "refunded",
            ],
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Order status must be a known enum value",
        ),
        AcceptedValuesRule(
            rule_id="bronze_currency_valid",
            rule_version="1.0",
            column_name="currency",
            accepted_values=["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD"],
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Currency must be a supported ISO code",
        ),
        PositiveNumericRule(
            rule_id="bronze_quantity_positive",
            rule_version="1.0",
            column_name="quantity",
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Quantity should be positive (negative = return?)",
        ),
        PositiveNumericRule(
            rule_id="bronze_unit_price_positive",
            rule_version="1.0",
            column_name="unit_price",
            severity=Severity.WARNING,
            layer=Layer.BRONZE,
            description="Unit price should be positive",
        ),
        SchemaDriftRule(
            rule_id="bronze_schema_drift",
            rule_version="1.0",
            expected_columns=[
                "order_id", "customer_id", "product_id", "order_status",
                "quantity", "unit_price", "currency", "order_timestamp",
            ],
            allow_extra_columns=False,
            severity=Severity.CRITICAL,
            layer=Layer.BRONZE,
            description="Detect unexpected schema changes in raw events",
        ),
    ]


def get_silver_rules() -> List[BaseRuleConfig]:
    """
    Default rule configurations for Silver layer validation.
    
    """
    return [
        NotNullRule(
            rule_id="silver_customer_id_not_null",
            rule_version="1.0",
            column_name="customer_id",
            severity=Severity.CRITICAL,
            layer=Layer.SILVER,
            description="Customer ID must not be null in Silver (quarantined in Bronze)",
        ),
        UniqueKeyRule(
            rule_id="silver_order_id_unique",
            rule_version="1.0",
            columns=["order_id"],
            severity=Severity.CRITICAL,
            layer=Layer.SILVER,
            description="Order ID must be unique in Silver (deduplicated from Bronze)",
        ),
        TimestampFreshnessRule(
            rule_id="silver_freshness_check",
            rule_version="1.0",
            column_name="ingestion_timestamp",
            max_age_hours=4.0,
            severity=Severity.WARNING,
            layer=Layer.SILVER,
            description="Silver data should not be older than 4 hours",
        ),
        DuplicateDetectionRule(
            rule_id="silver_dedup_orders",
            rule_version="1.0",
            key_columns=["order_id"],
            order_by_column="order_timestamp",
            keep="last",
            severity=Severity.CRITICAL,
            layer=Layer.SILVER,
            description="Deduplicate orders by order_id, keep latest",
        ),
        DistributionAnomalyRule(
            rule_id="silver_price_distribution",
            rule_version="1.0",
            column_name="unit_price",
            z_score_threshold=3.0,
            max_percentile_drift_pct=25.0,
            severity=Severity.WARNING,
            layer=Layer.SILVER,
            description="Detect abnormal price distribution changes",
        ),
        DistributionAnomalyRule(
            rule_id="silver_quantity_distribution",
            rule_version="1.0",
            column_name="quantity",
            z_score_threshold=3.0,
            max_percentile_drift_pct=30.0,
            severity=Severity.WARNING,
            layer=Layer.SILVER,
            description="Detect abnormal quantity distribution changes",
        ),
    ]


def get_gold_rules() -> List[BaseRuleConfig]:
    """
    Default rule configurations for Gold layer validation.
    
    """
    return [
        RowCountAnomalyRule(
            rule_id="gold_daily_revenue_row_count",
            rule_version="1.0",
            min_deviation_pct=-30.0,
            max_deviation_pct=50.0,
            lookback_runs=10,
            severity=Severity.CRITICAL,
            layer=Layer.GOLD,
            description="Detect sudden drops or spikes in daily revenue row count",
        ),
        TimestampFreshnessRule(
            rule_id="gold_freshness_check",
            rule_version="1.0",
            column_name="computed_at",
            max_age_hours=2.0,
            severity=Severity.CRITICAL,
            layer=Layer.GOLD,
            description="Gold data must be less than 2 hours old (SLA)",
        ),
        DistributionAnomalyRule(
            rule_id="gold_revenue_distribution",
            rule_version="1.0",
            column_name="total_revenue",
            z_score_threshold=2.5,
            max_percentile_drift_pct=20.0,
            severity=Severity.CRITICAL,
            layer=Layer.GOLD,
            description="Detect aggregation skew in revenue numbers",
        ),
    ]
