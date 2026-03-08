"""
Validation result dataclass — standardized output for all rules.

This module defines the ValidationResult structure used by all rules to provide 
consistent output for metrics collection, quarantine routing, and observability.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    """
    Standardized result from any validation rule execution.
    """
    # Rule identification
    rule_id: str
    rule_name: str
    rule_version: str = "1.0"
    
    # Result
    passed: bool = True
    severity: str = "warning"
    
    # Metrics
    total_records: int = 0
    failed_records: int = 0
    failure_rate: float = 0.0
    
    # Optional DataFrame containing records that failed validation.
    failed_df: Optional[DataFrame] = None
    
    # Rule-specific metadata (flexible)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Examples:
    # - schema_drift: {"added_columns": [...], "removed_columns": [...]}
    # - distribution: {"current_mean": 50.0, "historical_mean": 48.0, "z_score": 1.2}
    # - freshness: {"max_age_hours": 5.2, "sla_hours": 4.0}
    
    # Execution context
    dataset_name: str = ""
    layer: str = ""
    run_timestamp: str = field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )
    execution_time_ms: float = 0.0
    
    @property
    def pass_rate(self) -> float:
        """Percentage of records that passed."""
        if self.total_records == 0:
            return 100.0
        return ((self.total_records - self.failed_records) / self.total_records) * 100.0
    
    def summary(self) -> str:
        """Human-readable summary of the result."""
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return (
            f"{status} | {self.rule_name} ({self.rule_id})\n"
            f"  Records: {self.total_records:,} total, {self.failed_records:,} failed "
            f"({self.failure_rate:.2%})\n"
            f"  Severity: {self.severity} | Layer: {self.layer}\n"
            f"  Execution: {self.execution_time_ms:.0f}ms"
        )
    
    def to_metrics_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for observability metrics storage.
        """
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "passed": self.passed,
            "severity": self.severity,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": self.failure_rate,
            "pass_rate": self.pass_rate,
            "dataset_name": self.dataset_name,
            "layer": self.layer,
            "run_timestamp": self.run_timestamp,
            "execution_time_ms": self.execution_time_ms,
            "metadata": str(self.metadata),
        }
