"""
Rule: Timestamp Freshness Check

This module measures the age of the newest record in a dataset against a configured
SLA threshold. It is used to detect stalls in data ingestion or processing.
"""

import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import TimestampFreshnessRule
from rules.validation_result import ValidationResult


def timestamp_freshness_check(
    df: DataFrame,
    rule: TimestampFreshnessRule,
    dataset_name: str = "",
    reference_time: datetime = None,
) -> ValidationResult:
    """
    Check that the newest record in the dataset is within the freshness SLA.
    
    Args:
        df: DataFrame to validate
        rule: TimestampFreshnessRule configuration
        dataset_name: Name of dataset being validated
        reference_time: Optional reference time (defaults to current time)
    
    Returns:
        ValidationResult (aggregate check — no failed_df)
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    if total == 0:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": "DataFrame is empty — no records to check freshness"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Use the max timestamp from the data for comparison.
    col_type = dict(df.dtypes).get(rule.column_name)
    
    if col_type == "string":
        # Attempt to parse string timestamps
        max_ts_row = df.agg(
            F.max(F.to_timestamp(F.col(rule.column_name))).alias("max_ts")
        ).collect()[0]
    elif col_type in ("timestamp", "date"):
        max_ts_row = df.agg(
            F.max(F.col(rule.column_name)).alias("max_ts")
        ).collect()[0]
    else:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' type {col_type} is not a timestamp"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    max_ts = max_ts_row["max_ts"]
    
    if max_ts is None:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="timestamp_freshness_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": "All timestamp values are null or unparseable"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Calculate age
    if reference_time is None:
        reference_time = datetime.utcnow()
    
    age_seconds = (reference_time - max_ts).total_seconds()
    age_hours = age_seconds / 3600.0
    
    # Check against SLA
    passed = age_hours <= rule.max_age_hours
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="timestamp_freshness_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=0 if passed else total,  # Aggregate check — all or nothing
        failure_rate=0.0 if passed else 1.0,
        failed_df=None,  # No per-record failures — this is aggregate
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "max_timestamp": str(max_ts),
            "reference_time": str(reference_time),
            "age_hours": round(age_hours, 2),
            "sla_hours": rule.max_age_hours,
            "breach_hours": round(age_hours - rule.max_age_hours, 2) if not passed else 0,
        },
        execution_time_ms=elapsed_ms,
    )
