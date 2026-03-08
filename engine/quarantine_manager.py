"""
Quarantine Manager — routes failed records to quarantine Delta tables.

This module manages the routing of records that fail critical validation rules
to dedicated quarantine Delta tables, preserving them for investigation and 
potential reprocessing.
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_configs import PipelineConfig
from utils.path_resolver import PATHS


class QuarantineManager:
    """
    Routes failed records to quarantine Delta tables with metadata.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
    
    def quarantine_records(
        self,
        failed_df: DataFrame,
        layer: str,
        run_id: str,
        failure_reasons: Dict[str, str] = None,
        dataset_name: str = "",
    ) -> int:
        """
        Write failed records to the quarantine Delta table with metadata.
        
        Args:
            failed_df: DataFrame of records to quarantine
            layer: Medallion layer where failure was detected
            run_id: Current pipeline run ID
            failure_reasons: Optional dict of rule_name → failure reason
            dataset_name: Name of the source dataset
        
        Returns:
            Number of records quarantined
        """
        if failed_df is None or failed_df.rdd.isEmpty():
            return 0
        
        # Add quarantine metadata
        quarantine_df = (
            failed_df
            .withColumn("_quarantine_timestamp", F.current_timestamp())
            .withColumn("_quarantine_run_id", F.lit(run_id))
            .withColumn("_quarantine_layer", F.lit(layer))
            .withColumn("_quarantine_dataset", F.lit(dataset_name))
            .withColumn(
                "_quarantine_reasons",
                F.lit(str(failure_reasons) if failure_reasons else "multiple_rules")
            )
        )
        
        # Cap records
        max_records = self.config.quarantine.max_records_to_store
        record_count = quarantine_df.count()
        
        if record_count > max_records:
            # Take a random sample if threshold exceeded.
            fraction = max_records / record_count
            quarantine_df = quarantine_df.sample(fraction=fraction, seed=42).limit(max_records)
            record_count = max_records
        
        # Get quarantine path for the layer
        paths = self.config.paths.get_layer_paths(layer)
        quarantine_path = paths.get("quarantine", "")
        
        if not quarantine_path:
            print(f"[QuarantineManager] WARNING: No quarantine path for layer '{layer}'")
            return 0
        
        # Write to Delta table (append mode — never overwrite quarantine).
        try:
            (
                quarantine_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path)
            )
            
            print(f"[QuarantineManager] Quarantined {record_count} records "
                  f"from {dataset_name} ({layer}) → {quarantine_path}")
            
        except Exception as e:
            print(f"[QuarantineManager] ERROR writing quarantine: {e}")
            return 0
        
        return record_count
    
    def get_quarantine_summary(
        self,
        layer: str,
    ) -> Optional[DataFrame]:
        """
        Read quarantine table and provide summary statistics.
        """
        paths = self.config.paths.get_layer_paths(layer)
        quarantine_path = paths.get("quarantine", "")
        
        try:
            qdf = self.spark.read.format("delta").load(quarantine_path)
            
            summary = (
                qdf
                .groupBy("_quarantine_layer", "_quarantine_dataset", "_quarantine_reasons")
                .agg(
                    F.count("*").alias("record_count"),
                    F.min("_quarantine_timestamp").alias("first_quarantined"),
                    F.max("_quarantine_timestamp").alias("last_quarantined"),
                    F.countDistinct("_quarantine_run_id").alias("affected_runs"),
                )
                .orderBy(F.desc("record_count"))
            )
            
            return summary
            
        except Exception as e:
            print(f"[QuarantineManager] No quarantine data for {layer}: {e}")
            return None
