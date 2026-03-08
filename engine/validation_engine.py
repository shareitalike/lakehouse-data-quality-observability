"""
Validation Engine — orchestrates rule execution per pipeline layer.

This module provides the central orchestration logic for executing validation 
rules across the Medallion architecture, aggregating results and determining
routing decisions (e.g., quarantine).
"""

import time
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from config.rule_configs import (
    BaseRuleConfig, Severity, get_bronze_rules,
    get_silver_rules, get_gold_rules,
)
from config.pipeline_configs import PipelineConfig
from rules.validation_result import ValidationResult
from rules.rule_registry import execute_rules


class ValidationEngine:
    """
    Orchestrates validation rule execution and result aggregation.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
        self._run_id = str(uuid.uuid4())
        self._results_cache: Dict[str, List[ValidationResult]] = {}
    
    @property
    def run_id(self) -> str:
        return self._run_id
    
    def new_run(self) -> str:
        """Start a new validation run with a fresh run_id."""
        self._run_id = str(uuid.uuid4())
        self._results_cache = {}
        return self._run_id
    
    def validate(
        self,
        df: DataFrame,
        rules: List[BaseRuleConfig],
        dataset_name: str,
        **kwargs,
    ) -> List[ValidationResult]:
        """
        Execute all rules against a DataFrame and return results.
        
        
        Args:
            df: DataFrame to validate
            rules: List of rule configurations
            dataset_name: Name of the dataset
            **kwargs: Extra arguments for specific rules
        
        Returns:
            List of ValidationResult objects
        """
        results = execute_rules(df, rules, dataset_name, **kwargs)
        
        # Stamp results with run_id
        for result in results:
            result.metadata["run_id"] = self._run_id
        
        # Cache results for later processing
        self._results_cache[dataset_name] = results
        
        return results
    
    def validate_bronze(
        self,
        df: DataFrame,
        dataset_name: str = "bronze_raw_events",
        custom_rules: List[BaseRuleConfig] = None,
    ) -> List[ValidationResult]:
        """
        Run Bronze-layer validation rules.
        
        Bronze validation focuses on: schema integrity, critical nulls,
        and data shape. Business logic validation happens in Silver.
        """
        rules = custom_rules or get_bronze_rules()
        return self.validate(df, rules, dataset_name)
    
    def validate_silver(
        self,
        df: DataFrame,
        dataset_name: str = "silver_orders",
        custom_rules: List[BaseRuleConfig] = None,
        **kwargs,
    ) -> List[ValidationResult]:
        """
        Run Silver-layer validation rules.
        
        Silver validation focuses on: business logic, referential integrity,
        deduplication, and distribution analysis.
        """
        rules = custom_rules or get_silver_rules()
        return self.validate(df, rules, dataset_name, **kwargs)
    
    def validate_gold(
        self,
        df: DataFrame,
        dataset_name: str = "gold_daily_revenue",
        custom_rules: List[BaseRuleConfig] = None,
        **kwargs,
    ) -> List[ValidationResult]:
        """
        Run Gold-layer validation rules.
        
        Gold validation focuses on: row count anomalies, freshness SLA,
        and aggregation skew detection.
        """
        rules = custom_rules or get_gold_rules()
        return self.validate(df, rules, dataset_name, **kwargs)
    
    def get_critical_failures(
        self,
        results: List[ValidationResult],
    ) -> List[ValidationResult]:
        """Return only critical severity failures."""
        return [
            r for r in results
            if not r.passed and r.severity == "critical"
        ]
    
    def get_warnings(
        self,
        results: List[ValidationResult],
    ) -> List[ValidationResult]:
        """Return only warning severity failures."""
        return [
            r for r in results
            if not r.passed and r.severity == "warning"
        ]
    
    def should_quarantine(
        self,
        results: List[ValidationResult],
        df: DataFrame,
    ) -> bool:
        """
        Determine if records should be quarantined based on results.
        """
        if not self.config.quarantine.enabled:
            return False
        
        critical_failures = self.get_critical_failures(results)
        return len(critical_failures) > 0
    
    def get_quarantine_records(
        self,
        results: List[ValidationResult],
    ) -> Optional[DataFrame]:
        """
        Collect all failed records from critical rules for quarantine.
        """
        failed_dfs = []
        for result in results:
            if (not result.passed and 
                result.severity == "critical" and 
                result.failed_df is not None):
                failed_dfs.append(result.failed_df)
        
        if not failed_dfs:
            return None
        
        # Union all failed DataFrames using unionByName (allow schema drift).
        union_df = failed_dfs[0]
        for fd in failed_dfs[1:]:
            try:
                union_df = union_df.unionByName(fd, allowMissingColumns=True)
            except Exception:
                # If union fails (schema mismatch), skip this source
                continue
        
        # Deduplicate quarantine records
        union_df = union_df.dropDuplicates()
        
        # Cap at max records
        max_records = self.config.quarantine.max_records_to_store
        if union_df.count() > max_records:
            union_df = union_df.limit(max_records)
        
        return union_df
    
    def generate_summary(
        self,
        results: List[ValidationResult],
    ) -> Dict[str, Any]:
        """
        Generate a human-readable validation summary.
        """
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
        critical = len(self.get_critical_failures(results))
        warnings = len(self.get_warnings(results))
        
        total_time = sum(r.execution_time_ms for r in results)
        
        return {
            "run_id": self._run_id,
            "timestamp": datetime.utcnow().isoformat(),
            "total_rules": total,
            "passed": passed,
            "failed": failed,
            "critical_failures": critical,
            "warnings": warnings,
            "pass_rate": (passed / total * 100) if total > 0 else 100.0,
            "total_execution_ms": round(total_time, 0),
            "details": [r.to_metrics_dict() for r in results],
        }
