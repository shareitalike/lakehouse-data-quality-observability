"""
Rule: Distribution Anomaly Check

This module detects abnormal distribution changes in numeric columns by comparing current
run statistics against historical baselines. It uses a dual detection approach:
1. Z-Score analysis: Identifies significant shifts in the mean.
2. Percentile Drift: Monitors changes in the statistical shape of the data (P25, P50, P75, etc.).

This is essential for identifying subtle data quality issues like price manipulation, 
currency conversion errors, or feature drift in machine learning models.
"""

import time
import json
from typing import List, Dict, Optional, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.rule_configs import DistributionAnomalyRule
from rules.validation_result import ValidationResult


def _compute_statistics(
    df: DataFrame,
    column: str,
    percentile_bands: List[float],
    use_approx: bool = True,
    relative_error: float = 0.01,
) -> Dict[str, Any]:
    """
    Compute comprehensive statistics for a numeric column.
    
    # Compute statistics in a single pass
    # approxQuantile requires a separate call as it is not a standard aggregate function.
    """
    # Basic statistics in one pass
    stats_row = df.agg(
        F.count(column).alias("count"),
        F.mean(column).alias("mean"),
        F.stddev(column).alias("stddev"),
        F.min(column).alias("min_val"),
        F.max(column).alias("max_val"),
        F.skewness(column).alias("skewness"),
        F.kurtosis(column).alias("kurtosis"),
    ).collect()[0]
    
    stats = {
        "count": int(stats_row["count"]) if stats_row["count"] else 0,
        "mean": float(stats_row["mean"]) if stats_row["mean"] else None,
        "stddev": float(stats_row["stddev"]) if stats_row["stddev"] else None,
        "min": float(stats_row["min_val"]) if stats_row["min_val"] else None,
        "max": float(stats_row["max_val"]) if stats_row["max_val"] else None,
        "skewness": float(stats_row["skewness"]) if stats_row["skewness"] else None,
        "kurtosis": float(stats_row["kurtosis"]) if stats_row["kurtosis"] else None,
    }
    
    # Compute percentiles
    # Use approxQuantile for performance at scale.
    if use_approx:
        percentiles = df.approxQuantile(column, percentile_bands, relative_error)
    else:
        # Exact quantiles for small datasets
        percentiles = df.approxQuantile(column, percentile_bands, 0.0)
    
    stats["percentiles"] = {
        f"p{int(p*100)}": v 
        for p, v in zip(percentile_bands, percentiles)
    }
    
    return stats


def _compute_z_score(
    current_mean: float,
    historical_mean: float,
    historical_stddev: float,
) -> Optional[float]:
    """
    Compute z-score of current mean against historical baseline.
    
    # Handle cases where historical standard deviation is zero or unavailable.
    """
    if historical_stddev is None or historical_stddev == 0:
        if current_mean == historical_mean:
            return 0.0
        else:
            return float('inf')  # Any deviation from constant is infinite z
    
    return (current_mean - historical_mean) / historical_stddev


def _compute_percentile_drift(
    current_percentiles: Dict[str, float],
    historical_percentiles: Dict[str, float],
) -> Dict[str, float]:
    """
    Compute percentage drift for each percentile band.
    
    Returns dict of {percentile_name: drift_pct}
    """
    drift = {}
    for key in current_percentiles:
        if key in historical_percentiles:
            hist_val = historical_percentiles[key]
            curr_val = current_percentiles[key]
            
            if hist_val is not None and hist_val != 0:
                pct_change = ((curr_val - hist_val) / abs(hist_val)) * 100
                drift[key] = round(pct_change, 2)
            elif hist_val == 0:
                drift[key] = 100.0 if curr_val != 0 else 0.0
            else:
                drift[key] = None
    
    return drift


def distribution_anomaly_check(
    current_df: DataFrame,
    rule: DistributionAnomalyRule,
    historical_stats: Dict[str, Any] = None,
    dataset_name: str = "",
) -> ValidationResult:
    """
    Detect abnormal distribution changes in a numeric column.
    
    Uses dual detection:
    1. Z-score: Is the mean significantly different from historical?
    2. Percentile drift: Has the distribution shape changed?
    
    Args:
        current_df: Current DataFrame to analyze
        rule: DistributionAnomalyRule configuration
        historical_stats: Statistics from previous runs. If None, establishes
                         baseline and returns pass.
                         Expected format: output of _compute_statistics()
        dataset_name: Name of dataset being validated
    
    Returns:
        ValidationResult with comprehensive distribution metadata
    """
    start_time = time.time()
    
    # Guard: check column exists  
    if rule.column_name not in current_df.columns:
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=False,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={"error": f"Column '{rule.column_name}' not found"},
            execution_time_ms=(time.time() - start_time) * 1000,
        )
    
    # Filter to non-null values for statistics
    analysis_df = current_df.filter(F.col(rule.column_name).isNotNull())
    record_count = analysis_df.count()
    total_count = current_df.count()
    
    # Check minimum sample size
    if record_count < rule.min_sample_size:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total_count,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "note": f"Skipped — only {record_count} non-null records, "
                        f"minimum required: {rule.min_sample_size}",
                "column_name": rule.column_name,
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Compute current statistics
    current_stats = _compute_statistics(
        analysis_df,
        rule.column_name,
        rule.percentile_bands,
        use_approx=rule.use_approx_quantile,
        relative_error=rule.relative_error,
    )
    
    # First run — establish baseline
    if historical_stats is None:
        elapsed_ms = (time.time() - start_time) * 1000
        return ValidationResult(
            rule_id=rule.rule_id,
            rule_name="distribution_anomaly_check",
            rule_version=rule.rule_version,
            passed=True,
            severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
            total_records=total_count,
            dataset_name=dataset_name,
            layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
            metadata={
                "column_name": rule.column_name,
                "baseline_established": True,
                "current_stats": current_stats,
                "note": "First run — baseline established, no comparison available",
            },
            execution_time_ms=elapsed_ms,
        )
    
    # Compare against historical baseline
    anomalies = []
    
    # 1. Z-score check
    z_score = _compute_z_score(
        current_stats["mean"],
        historical_stats.get("mean", 0),
        historical_stats.get("stddev", 0),
    )
    
    z_score_anomaly = abs(z_score) > rule.z_score_threshold if z_score is not None else False
    if z_score_anomaly:
        anomalies.append(
            f"Z-score anomaly: z={z_score:.2f} exceeds threshold ±{rule.z_score_threshold}"
        )
    
    # 2. Percentile drift check
    percentile_drift = {}
    max_drift = 0.0
    percentile_anomaly = False
    
    if "percentiles" in current_stats and "percentiles" in historical_stats:
        percentile_drift = _compute_percentile_drift(
            current_stats["percentiles"],
            historical_stats["percentiles"],
        )
        
        for pct_name, drift_val in percentile_drift.items():
            if drift_val is not None and abs(drift_val) > rule.max_percentile_drift_pct:
                percentile_anomaly = True
                anomalies.append(
                    f"Percentile drift: {pct_name} shifted {drift_val:.1f}% "
                    f"(threshold: ±{rule.max_percentile_drift_pct}%)"
                )
            if drift_val is not None:
                max_drift = max(max_drift, abs(drift_val))
    
    # Determine overall pass/fail
    is_anomaly = z_score_anomaly or percentile_anomaly
    passed = not is_anomaly
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    return ValidationResult(
        rule_id=rule.rule_id,
        rule_name="distribution_anomaly_check",
        rule_version=rule.rule_version,
        passed=passed,
        severity=rule.severity.value if hasattr(rule.severity, 'value') else str(rule.severity),
        total_records=total_count,
        failed_records=total_count if is_anomaly else 0,
        failure_rate=1.0 if is_anomaly else 0.0,
        failed_df=None,  # Aggregate check — distribution affects all records
        dataset_name=dataset_name,
        layer=rule.layer.value if hasattr(rule.layer, 'value') else str(rule.layer),
        metadata={
            "column_name": rule.column_name,
            "current_stats": current_stats,
            "historical_stats": historical_stats,
            "z_score": round(z_score, 4) if z_score is not None else None,
            "z_score_threshold": rule.z_score_threshold,
            "z_score_anomaly": z_score_anomaly,
            "percentile_drift": percentile_drift,
            "max_percentile_drift_pct": round(max_drift, 2),
            "percentile_threshold_pct": rule.max_percentile_drift_pct,
            "percentile_anomaly": percentile_anomaly,
            "anomalies": anomalies,
            "detection_method": "dual (z-score + percentile)",
            "used_approx_quantile": rule.use_approx_quantile,
        },
        execution_time_ms=elapsed_ms,
    )
