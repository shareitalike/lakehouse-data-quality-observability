"""
Rule: Schema Drift Detection

This module identifies structural changes in the dataset by comparing the actual 
DataFrame schema against an expected configuration. It reports added columns, 
missing columns, and data type mismatches.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from config.rule_configs import SchemaDriftRule
from rules.validation_result import ValidationResult


def schema_drift_detection(
    df: DataFrame,
    rule: SchemaDriftRule,
    dataset_name: str = "",
    expected_schema: StructType = None,
) -> ValidationResult:
    """
    Detect schema differences between actual data and expected schema.
    
    Args:
        df: DataFrame to validate
        rule: SchemaDriftRule configuration 
        dataset_name: Name of dataset being validated
        expected_schema: Optional StructType for type comparison.
                        If None, only column name comparison is performed.
    
    Returns:
        ValidationResult with drift details in metadata
    """
    start_time = time.time()
    
    total = df.count()
    
    # Get actual columns
    actual_columns = set(df.columns)
    expected_columns = set(rule.expected_columns)
    
    # Detect differences
    added_columns = list(actual_columns - expected_columns)
    removed_columns = list(expected_columns - actual_columns)
    
    # Type change detection (if expected_schema provided)
    type_changes = []
    if expected_schema:
        expected_types = {f.name: f.dataType.simpleString() for f in expected_schema.fields}
        actual_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        
        for col_name in (expected_columns & actual_columns):
            if col_name in expected_types and col_name in actual_types:
                if expected_types[col_name] != actual_types[col_name]:
                    type_changes.append({
                        "column": col_name,
                        "expected_type": expected_types[col_name],
                        "actual_type": actual_types[col_name],
                    })
    
    # Determine if drift is detected
    has_drift = False
    drift_reasons = []
    
    if removed_columns and rule.alert_on_missing:
        has_drift = True
        drift_reasons.append(f"Missing columns: {removed_columns}")
    
    if added_columns and not rule.allow_extra_columns:
        has_drift = True
        drift_reasons.append(f"Unexpected columns: {added_columns}")
    
    if type_changes and rule.alert_on_type_change:
        has_drift = True
        drift_reasons.append(f"Type changes: {type_changes}")
    
    passed = not has_drift
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="schema_drift_detection",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=total if has_drift else 0,  # Schema drift affects ALL records
        failure_rate=1.0 if has_drift else 0.0,
        failed_df=None,  # No per-record failures for schema issues
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "expected_columns": sorted(list(expected_columns)),
            "actual_columns": sorted(list(actual_columns)),
            "added_columns": sorted(added_columns),
            "removed_columns": sorted(removed_columns),
            "type_changes": type_changes,
            "allow_extra_columns": rule.allow_extra_columns,
            "has_drift": has_drift,
            "drift_reasons": drift_reasons,
            "column_count_expected": len(expected_columns),
            "column_count_actual": len(actual_columns),
        },
        execution_time_ms=elapsed_ms,
    )
