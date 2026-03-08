"""
Rule: Unique Key Check

This module validates that a specified set of columns forms a unique key within the dataset.
It identifies groups of records with identical key combinations and calculates the number
of excess records that violate the uniqueness constraint.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import UniqueKeyRule
from rules.validation_result import ValidationResult


def unique_key_check(
    df: DataFrame,
    rule: UniqueKeyRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that columns form a unique key (no duplicate combinations).
    
    Args:
        df: DataFrame to validate
        rule: UniqueKeyRule configuration
        dataset_name: Name of the dataset being validated
    
    Returns:
        ValidationResult with duplicate key groups
    """
    start_time = time.time()
    
    # Guard: check all columns exist
    missing_cols = [c for c in rule.columns if c not in df.columns]
    if missing_cols:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="unique_key_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Missing columns: {missing_cols}"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    
    # Find duplicate key groups
    # Find duplicate key groups
    dup_groups = (
        df.groupBy(rule.columns)
        .agg(F.count("*").alias("_dup_count"))
        .filter(F.col("_dup_count") > 1)
    )
    
    dup_group_count = dup_groups.count()
    
    # Count total records involved in duplicates
    # Count total records involved in duplicates
    if dup_group_count > 0:
        total_dup_records = dup_groups.agg(F.sum("_dup_count")).collect()[0][0]
        # Total excess records (total dups - one per group = extras that shouldn't exist)
        excess_records = int(total_dup_records - dup_group_count)
    else:
        total_dup_records = 0
        excess_records = 0
    
    # Build failed DataFrame: all records that participate in duplicates
    if dup_group_count > 0:
        # Join back to original to get full records
        failed_df = df.join(
            dup_groups.select(rule.columns),
            on=rule.columns,
            how="inner"
        )
    else:
        failed_df = None
    
    failure_rate = excess_records / total if total > 0 else 0.0
    passed = dup_group_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="unique_key_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=excess_records,
        failure_rate=failure_rate,
        failed_df=failed_df,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "key_columns": rule.columns,
            "duplicate_groups": dup_group_count,
            "total_duplicate_records": int(total_dup_records) if total_dup_records else 0,
            "excess_records": excess_records,
        },
        execution_time_ms=elapsed_ms,
    )
