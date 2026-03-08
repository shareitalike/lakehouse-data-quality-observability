"""
Rule: Not Null Check

This module provides validation logic to ensure that a specified column does not contain null values.
It implements a non-destructive filter that identifies records violating the constraint without
modifying the original dataset.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import NotNullRule
from rules.validation_result import ValidationResult


def not_null_check(
    df: DataFrame,
    rule: NotNullRule,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that a column has no null values.
    
    Args:
        df: DataFrame to validate
        rule: NotNullRule configuration
        dataset_name: Name of the dataset being validated
    
    Returns:
        ValidationResult with failed records (rows where column is null)
    """
    start_time = time.time()
    
    # Guard: check column exists
    if rule.column_name not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="not_null_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=0,
            failed_records=0,
            failure_rate=0.0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "error": f"Column '{rule.column_name}' does not exist in DataFrame",
                "available_columns": df.columns,
            },
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Count total records
    total = df.count()
    
    # Find null records
    failed_df = df.filter(F.col(rule.column_name).isNull())
    failed_count = failed_df.count()
    
    # Calculate failure rate
    failure_rate = failed_count / total if total > 0 else 0.0
    
    # Determine pass/fail
    passed = failed_count == 0
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="not_null_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=failed_count,
        failure_rate=failure_rate,
        failed_df=failed_df if failed_count > 0 else None,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "null_count": failed_count,
            "null_rate": failure_rate,
        },
        execution_time_ms=elapsed_ms,
    )
