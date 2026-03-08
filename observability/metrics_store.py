"""
Metrics Store — writes observability metrics to Delta table.

This module manages the persistence of data quality metrics to append-only Delta 
tables, enabling historical trend analysis, audit trails, and compliance 
reporting for the data pipeline.
"""

from typing import List, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from utils.path_resolver import PATHS
from rules.validation_result import ValidationResult
from observability.metrics_collector import MetricsCollector


class MetricsStore:
    """
    Persists observability metrics to append-only Delta table.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        metrics_path: str = None,
    ):
        self.spark = spark
        self.metrics_path = metrics_path or PATHS.observability_metrics
        self.collector = MetricsCollector(spark)
    
    def write_metrics(
        self,
        results: List[ValidationResult],
        run_id: str,
        pipeline_name: str = "",
    ) -> int:
        """
        Write validation results as metrics to the Delta table.
        
        
        Args:
            results: List of ValidationResult objects
            run_id: Unique identifier for this pipeline run
            pipeline_name: Name of the pipeline
        
        Returns:
            Number of metric rows written
        """
        try:
            # Collect metrics
            metrics_df = self.collector.collect_from_results(
                results, run_id, pipeline_name
            )
            
            metric_count = metrics_df.count()
            
            if metric_count == 0:
                print("[MetricsStore] No metrics to write")
                return 0
            
            # Write to Delta in append-only mode to preserve history.
            (
                metrics_df
                .write
                .format("delta")
                .mode("append")
                .save(self.metrics_path)
            )
            
            print(f"[MetricsStore] Wrote {metric_count} metrics to {self.metrics_path}")
            return metric_count
            
        except Exception as e:
            # Metrics write failures should not fail the primary pipeline.
            print(f"[MetricsStore] ERROR writing metrics: {e}")
            return 0
    
    def read_metrics(
        self,
        dataset_name: str = None,
        layer: str = None,
        rule_name: str = None,
        last_n_days: int = None,
    ) -> Optional[DataFrame]:
        """
        Read metrics from the Delta table with optional filters.
        
        Args:
            dataset_name: Filter by dataset
            layer: Filter by Medallion layer
            rule_name: Filter by rule name
            last_n_days: Filter to last N days
        
        Returns:
            Filtered metrics DataFrame
        """
        try:
            df = self.spark.read.format("delta").load(self.metrics_path)
            
            if dataset_name:
                df = df.filter(F.col("dataset_name") == dataset_name)
            if layer:
                df = df.filter(F.col("layer") == layer)
            if rule_name:
                df = df.filter(F.col("rule_name") == rule_name)
            if last_n_days:
                cutoff = F.date_sub(F.current_date(), last_n_days)
                df = df.filter(F.col("run_timestamp") >= cutoff)
            
            return df
            
        except Exception as e:
            print(f"[MetricsStore] Error reading metrics: {e}")
            return None
    
    def get_trend(
        self,
        metric_name: str,
        dataset_name: str,
        rule_name: str = None,
        last_n_runs: int = 10,
    ) -> Optional[DataFrame]:
        """
        Get metric trend over recent runs.
        
        """
        df = self.read_metrics(dataset_name=dataset_name, rule_name=rule_name)
        if df is None:
            return None
        
        trend = (
            df
            .filter(F.col("metric_name") == metric_name)
            .orderBy(F.desc("run_timestamp"))
            .limit(last_n_runs)
            .select(
                "run_id", "run_timestamp", "metric_name",
                "metric_value", "passed", "rule_name"
            )
            .orderBy("run_timestamp")
        )
        
        return trend
    
    def get_historical_row_counts(
        self,
        dataset_name: str,
        last_n_runs: int = 10,
    ) -> List[int]:
        """
        Get historical row counts for row count anomaly detection.
        
        """
        df = self.read_metrics(dataset_name=dataset_name)
        if df is None:
            return []
        
        counts_df = (
            df
            .filter(F.col("metric_name") == "row_count")
            .orderBy(F.desc("run_timestamp"))
            .limit(last_n_runs)
            .select("metric_value")
            .orderBy("run_timestamp")
        )
        
        rows = counts_df.collect()
        return [int(r["metric_value"]) for r in rows]
