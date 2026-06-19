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
    
    
    # NOTE: What happens when a critical rule fails?"
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
    
    # NOTE: How do you handle clock skew in freshness checks?"
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
    
    
    # NOTE: Why separate duplicate detection from unique key check?"
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
    
    
    # NOTE: What's the difference between schema enforcement and
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
    
    # NOTE: Why percentage-based instead of absolute thresholds?"
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
    
    # NOTE: Why do you use both z-score and percentile detection?"
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
    Load rule configurations for Bronze layer validation from YAML.
    """
    from config.yaml_parser import load_rules_from_yaml
    import os
    
    rules_path = os.path.join(os.path.dirname(__file__), "rules", "bronze_rules.yaml")
    return load_rules_from_yaml(rules_path)


def get_silver_rules() -> List[BaseRuleConfig]:
    """
    Load rule configurations for Silver layer validation from YAML.
    """
    from config.yaml_parser import load_rules_from_yaml
    import os
    
    rules_path = os.path.join(os.path.dirname(__file__), "rules", "silver_rules.yaml")
    return load_rules_from_yaml(rules_path)


def get_gold_rules() -> List[BaseRuleConfig]:
    """
    Load rule configurations for Gold layer validation from YAML.
    """
    from config.yaml_parser import load_rules_from_yaml
    import os
    
    rules_path = os.path.join(os.path.dirname(__file__), "rules", "gold_rules.yaml")
    return load_rules_from_yaml(rules_path)
