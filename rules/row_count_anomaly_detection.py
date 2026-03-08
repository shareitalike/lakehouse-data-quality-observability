"""
Rule: Row Count Anomaly Detection

This module detects unexpected changes in data volume by comparing the current 
dataset's row count against a historical baseline. It identifies anomalies
based on percentage deviation thresholds.
"""

import time
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import RowCountAnomalyRule
from rules.validation_result import ValidationResult


def row_count_anomaly_detection(
    df: DataFrame,
    rule: RowCountAnomalyRule,
    historical_counts: List[int] = None,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Detect anomalous row count compared to historical baseline.
    
    Args:
        df: DataFrame to validate
        rule: RowCountAnomalyRule configuration
        historical_counts: List of row counts from previous pipeline runs.
                          If None or empty, this is treated as the first run
                          and the check always passes (establishes baseline).
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with anomaly details in metadata
    """
    start_time = time.time()
    
    current_count = df.count()
    
    # Establish baseline on first run.
    if not historical_counts:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="row_count_anomaly_detection",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=current_count,
            failed_records=0,
            failure_rate=0.0,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "current_count": current_count,
                "historical_counts": [],
                "baseline_established": True,
                "note": "First run — establishing baseline, no anomaly detection possible",
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Use last N runs for baseline
    recent_counts = historical_counts[-rule.lookback_runs:]
    
    # Calculate baseline statistics
    baseline_mean = sum(recent_counts) / len(recent_counts)
    baseline_min = min(recent_counts)
    baseline_max = max(recent_counts)
    
    # Standard deviation for context
    if len(recent_counts) > 1:
        variance = sum((x - baseline_mean) ** 2 for x in recent_counts) / len(recent_counts)
        baseline_stddev = variance ** 0.5
    else:
        baseline_stddev = 0.0
    
    # Calculate deviation
    if baseline_mean > 0:
        deviation_pct = ((current_count - baseline_mean) / baseline_mean) * 100
    else:
        deviation_pct = 100.0 if current_count > 0 else 0.0
    
    # Check against thresholds
    # Identify anomalies based on deviation thresholds.
    is_anomaly = (
        deviation_pct < rule.min_deviation_pct or
        deviation_pct > rule.max_deviation_pct
    )
    
    anomaly_type = None
    if deviation_pct < rule.min_deviation_pct:
        anomaly_type = "count_drop"
    elif deviation_pct > rule.max_deviation_pct:
        anomaly_type = "count_spike"
    
    passed = not is_anomaly
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="row_count_anomaly_detection",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=current_count,
        failed_records=current_count if is_anomaly else 0,
        failure_rate=1.0 if is_anomaly else 0.0,
        failed_df=None,  # Aggregate check — no per-record failures
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "current_count": current_count,
            "baseline_mean": round(baseline_mean, 1),
            "baseline_stddev": round(baseline_stddev, 1),
            "baseline_min": baseline_min,
            "baseline_max": baseline_max,
            "deviation_pct": round(deviation_pct, 2),
            "min_threshold_pct": rule.min_deviation_pct,
            "max_threshold_pct": rule.max_deviation_pct,
            "lookback_runs": len(recent_counts),
            "is_anomaly": is_anomaly,
            "anomaly_type": anomaly_type,
            "historical_counts": recent_counts,
        },
        execution_time_ms=elapsed_ms,
    )
