"""
Rule: Referential Integrity Check

This module validates that all foreign key values in a child dataset exist in a 
corresponding parent/reference dataset. It uses a left anti-join for efficient 
detection of orphan records.
"""

import time
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import ReferentialIntegrityRule
from rules.validation_result import ValidationResult


def referential_integrity_check(
    df: DataFrame,
    rule: ReferentialIntegrityRule,
    parent_df: DataFrame,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Check that all foreign key values in child table exist in parent table.
    
    Args:
        df: Child DataFrame to validate 
        rule: ReferentialIntegrityRule configuration
        parent_df: Parent/reference DataFrame containing valid values
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with orphan records (FK violations)
    """
    start_time = time.time()
    
    # Guard: check columns exist
    if rule.child_column not in df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="referential_integrity_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Child column '{rule.child_column}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    if rule.parent_column not in parent_df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="referential_integrity_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Parent column '{rule.parent_column}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    total = df.count()
    parent_count = parent_df.count()
    
    # Find child records with no matching parent using anti-join.
    # Anti-join is more efficient as Spark only needs to find non-matches.
    
    # Handle column name mismatch: rename parent column to match child
    parent_key = parent_df.select(
        F.col(rule.parent_column).alias(rule.child_column)
    ).distinct()
    
    # Auto-broadcast if parent table is small to avoid shuffle join.
    if parent_count < 10000:
        parent_key = F.broadcast(parent_key)
    
    orphans_df = df.join(
        parent_key,
        on=rule.child_column,
        how="left_anti"
    )
    
    # Exclude nulls from orphan count (handled by not_null_check).
    orphans_df = orphans_df.filter(F.col(rule.child_column).isNotNull())
    
    orphan_count = orphans_df.count()
    failure_rate = orphan_count / total if total > 0 else 0.0
    passed = orphan_count == 0
    
    # Sample invalid FK values for debugging
    invalid_fk_sample = []
    if orphan_count > 0:
        sample = (
            orphans_df.select(rule.child_column)
            .distinct()
            .limit(20)
            .collect()
        )
        invalid_fk_sample = [row[0] for row in sample]
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="referential_integrity_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total,
        failed_records=orphan_count,
        failure_rate=failure_rate,
        failed_df=orphans_df if orphan_count > 0 else None,
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "child_column": rule.child_column,
            "parent_column": rule.parent_column,
            "parent_record_count": parent_count,
            "orphan_count": orphan_count,
            "invalid_fk_sample": invalid_fk_sample,
            "used_broadcast": parent_count < 10000,
        },
        execution_time_ms=elapsed_ms,
    )
