"""
Validation Engine — orchestrates rule execution per pipeline layer.

INTERVIEW: "Why build a custom Validation Engine instead of using DLT expectations?"
→ "Delta Live Tables (DLT) is great, but in 2022-2023 it locked you into proprietary 
   Databricks runtime features. By building our own `ValidationEngine` using native PySpark, 
   our framework is completely portable across Azure Databricks Job Clusters, Azure Synapse, 
   or local test runners."

INTERVIEW: "How do you avoid Spark shuffle bottlenecks when evaluating 10+ rules?"
→ "We offer a dual execution model:
   1. `validate()`: Uses the Strategy Pattern via Rule Registry for granular rule-by-rule 
      reporting, contract checking, and metric persistence.
   2. `validate_and_tag_single_pass()`: Production-optimized single-pass columnar evaluation. 
      It generates an array of failed rule codes in a single Spark stage using PySpark expressions, 
      avoiding multiple full table scans and union shuffles on 50M+ row batches."
"""

import time
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.rule_configs import (
    BaseRuleConfig, Severity, get_bronze_rules,
    get_silver_rules, get_gold_rules,
)
from config.pipeline_configs import PipelineConfig
from rules.validation_result import ValidationResult
from rules.rule_registry import execute_rules


class ValidationEngine:
    """
    Orchestrates validation rule execution, single-pass tagging, and result aggregation.
    
    INTERVIEW: "Explain the Single Responsibility Principle here."
    → "The ValidationEngine's ONLY job is to take a DataFrame and a list of rules, 
       run them, and return standardized `ValidationResult` objects and tagged DataFrames. 
       It doesn't read data or write to storage. This separation makes it highly testable."
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
    
    def validate_bronze(self, df: DataFrame, **kwargs) -> List[ValidationResult]:
        """Convenience method to execute standard Bronze validation rules."""
        rules = get_bronze_rules()
        return self.validate(df, rules, dataset_name="bronze_raw_events", **kwargs)
    
    def validate_silver(self, df: DataFrame, dataset_name: str = "silver_orders", **kwargs) -> List[ValidationResult]:
        """Convenience method to execute standard Silver validation rules."""
        rules = get_silver_rules()
        return self.validate(df, rules, dataset_name=dataset_name, **kwargs)
    
    def validate_gold(self, df: DataFrame, dataset_name: str = "gold_daily_metrics", **kwargs) -> List[ValidationResult]:
        """Convenience method to execute standard Gold validation rules."""
        rules = get_gold_rules()
        return self.validate(df, rules, dataset_name=dataset_name, **kwargs)
    
    def validate(
        self,
        df: DataFrame,
        rules: List[BaseRuleConfig],
        dataset_name: str,
        **kwargs,
    ) -> List[ValidationResult]:
        """
        Execute all rules against a DataFrame via Rule Registry and return results.
        """
        results = execute_rules(df, rules, dataset_name, **kwargs)
        
        # Stamp results with run_id
        for result in results:
            result.metadata["run_id"] = self._run_id
        
        # Cache results for summary generation
        self._results_cache[dataset_name] = results
        
        return results

    # =========================================================================
    # PRODUCTION PERFORMANCE OPTIMIZATION: SINGLE-PASS DQ TAGGING
    # =========================================================================
    def validate_and_tag_single_pass(
        self,
        df: DataFrame,
        layer: str = "bronze",
    ) -> DataFrame:
        """
        PRODUCTION GRADE: Evaluates all layer-specific data quality conditions in 
        A SINGLE PYSPARK PASS via columnar array expressions.
        
        INTERVIEW: "Why is Single-Pass Tagging superior to separate filter actions?"
        → "Separate rule executions trigger N actions/shuffles. Single-pass tagging 
           creates a `_dq_failures` array column in ONE Catalyst execution plan:
           `array_compact(array(when(cond1, 'ERR_01'), when(cond2, 'ERR_02'), ...))`
           and derives `_is_quarantined = size(_dq_failures) > 0`. Clean records 
           and quarantine records are split with ZERO duplicate table scans."
        """
        error_conditions = []
        
        if layer.lower() == "bronze":
            # Rule 1: Null or empty order_id (Critical)
            if "order_id" in df.columns:
                error_conditions.append(
                    F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("ERR_BRONZE_NULL_ORDER_ID"))
                )
            # Rule 2: Malformed timestamp check (Critical)
            if "order_timestamp" in df.columns:
                error_conditions.append(
                    F.when(F.col("order_timestamp").isNull() | (F.to_timestamp(F.col("order_timestamp")).isNull()), F.lit("ERR_BRONZE_INVALID_TIMESTAMP"))
                )
                
        elif layer.lower() == "silver":
            # Rule 1: Null order_id (Critical)
            if "order_id" in df.columns:
                error_conditions.append(
                    F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("ERR_SILVER_NULL_ORDER_ID"))
                )
            # Rule 2: Null customer_id (Critical in Silver)
            if "customer_id" in df.columns:
                error_conditions.append(
                    F.when(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), F.lit("ERR_SILVER_NULL_CUSTOMER_ID"))
                )
            # Rule 3: Negative or zero total_amount (Critical)
            if "total_amount" in df.columns:
                error_conditions.append(
                    F.when(F.col("total_amount").isNull() | (F.col("total_amount") <= 0), F.lit("ERR_SILVER_INVALID_AMOUNT"))
                )
            # Rule 4: Invalid Order Status
            if "order_status" in df.columns:
                valid_statuses = ["COMPLETED", "PENDING", "PROCESSING", "SHIPPED", "CANCELLED", "REFUNDED"]
                error_conditions.append(
                    F.when(~F.col("order_status").isin(valid_statuses) | F.col("order_status").isNull(), F.lit("ERR_SILVER_INVALID_STATUS"))
                )
            # Rule 5: Future timestamp check
            if "order_timestamp" in df.columns:
                error_conditions.append(
                    F.when(F.col("order_timestamp") > F.current_timestamp(), F.lit("ERR_SILVER_FUTURE_TIMESTAMP"))
                )

        if not error_conditions:
            return df.withColumn("_dq_failures", F.array().cast("array<string>")).withColumn("_is_quarantined", F.lit(False))

        # Single-pass columnar array generation
        tagged_df = (
            df
            .withColumn("_dq_failures", F.array_compact(F.array(*error_conditions)))
            .withColumn("_is_quarantined", F.size(F.col("_dq_failures")) > 0)
        )
        return tagged_df

    def should_quarantine(
        self,
        results: List[ValidationResult],
        df: Optional[DataFrame] = None,
    ) -> bool:
        """
        Check if any critical failures occurred that warrant quarantine routing.
        """
        criticals = self.get_critical_failures(results)
        return len(criticals) > 0

    def get_critical_failures(
        self,
        results: List[ValidationResult],
    ) -> List[ValidationResult]:
        """Return only critical severity failures."""
        return [
            r for r in results
            if not r.passed and r.severity == "critical"
        ]
    
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
                continue
        
        # Deduplicate quarantine records
        union_df = union_df.dropDuplicates()
        
        # Circuit Breaker: cap max records written
        max_records = self.config.quarantine.max_records_to_store
        if union_df.count() > max_records:
            union_df = union_df.limit(max_records)
        
        return union_df

    def generate_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate high-level summary dictionary for observability reporting."""
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        critical_failures = sum(1 for r in results if not r.passed and r.severity == "critical")
        warning_failures = sum(1 for r in results if not r.passed and r.severity == "warning")
        
        return {
            "run_id": self._run_id,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "critical_failures": critical_failures,
            "warning_failures": warning_failures,
            "pass_rate_pct": round((passed_rules / total_rules * 100), 2) if total_rules > 0 else 100.0,
            "timestamp": datetime.utcnow().isoformat(),
        }
