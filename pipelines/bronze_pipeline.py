"""
Bronze Pipeline — raw data ingestion with DQ validation.

This module implements the Bronze layer of the Medallion architecture, focusing on 
raw data ingestion from sources while applying non-destructive data quality 
validation and quarantine routing.
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_configs import PipelineConfig
from config.rule_configs import get_bronze_rules
from config.layer_schemas import BRONZE_RAW_EVENTS_CONTRACT
from data_generation.bronze_generator import generate_clean_bronze_data, get_product_reference_df
from data_generation.issue_injector import inject_all_issues, InjectionConfig
from engine.validation_engine import ValidationEngine
from engine.quarantine_manager import QuarantineManager
from engine.contract_enforcer import ContractEnforcer
from observability.metrics_store import MetricsStore
from schemas.bronze_schemas import BRONZE_RAW_EVENTS_SCHEMA


class BronzePipeline:
    """
    Bronze layer pipeline: ingest → validate → store → quarantine.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig = None,
    ):
        self.spark = spark
        self.config = config or PipelineConfig.default()
        self.engine = ValidationEngine(spark, config)
        self.quarantine_mgr = QuarantineManager(spark, config)
        self.metrics_store = MetricsStore(spark)
        self.contract_enforcer = ContractEnforcer(spark)
        self.run_id = str(uuid.uuid4())
    
    def generate_data(
        self,
        inject_issues: bool = True,
    ) -> DataFrame:
        """
        Generate synthetic bronze data for processing.
        """
        print(f"\n{'='*60}")
        print(f"BRONZE PIPELINE — Data Generation")
        print(f"{'='*60}")
        
        # Generate clean data
        df = generate_clean_bronze_data(
            self.spark,
            num_records=self.config.num_records,
            seed=self.config.random_seed,
        )
        print(f"Generated {df.count()} clean records")
        
        # Inject issues if requested
        if inject_issues:
            injection_config = InjectionConfig(seed=self.config.random_seed)
            df = inject_all_issues(df, injection_config)
            
        # Watermark Filtering (Incremental Processing)
        if self.config.pipeline_date:
            print(f"Applying watermark filter for date: {self.config.pipeline_date}")
            df = df.filter(F.to_date(F.col("order_timestamp")) == F.lit(self.config.pipeline_date))
            print(f"Records after watermark filter: {df.count()}")
        
        return df
    
    def validate(
        self,
        df: DataFrame,
    ) -> Dict[str, Any]:
        """
        Run Bronze validation and return results with quarantine info.
        """
        print(f"\n{'='*60}")
        print(f"BRONZE PIPELINE — Validation (run_id: {self.run_id})")
        print(f"{'='*60}")
        
        # Run validation rules
        results = self.engine.validate_bronze(df)
        
        # Check data contract
        contract_result = self.contract_enforcer.enforce_contract(
            df, BRONZE_RAW_EVENTS_CONTRACT
        )
        
        # Determine quarantine
        quarantine_records = None
        quarantine_count = 0
        
        if self.engine.should_quarantine(results, df):
            quarantine_records = self.engine.get_quarantine_records(results)
            if quarantine_records is not None:
                quarantine_count = self.quarantine_mgr.quarantine_records(
                    quarantine_records,
                    layer="bronze",
                    run_id=self.run_id,
                    dataset_name="bronze_raw_events",
                )
        
        # Write observability metrics
        if self.config.enable_observability:
            self.metrics_store.write_metrics(
                results,
                run_id=self.run_id,
                pipeline_name=self.config.pipeline_name,
            )
        
        return {
            "run_id": self.run_id,
            "results": results,
            "contract_result": contract_result,
            "quarantine_count": quarantine_count,
            "summary": self.engine.generate_summary(results),
        }
    
    def write_to_delta(
        self,
        df: DataFrame,
        validation_results: Dict[str, Any] = None,
    ) -> str:
        """
        Write validated Bronze data to Delta table.
        """
        output_path = self.config.paths.bronze_raw
        
        # Enforce expected schema (drop extra columns like loyalty_tier from drift)
        expected_columns = [f.name for f in BRONZE_RAW_EVENTS_SCHEMA.fields]
        available_columns = [c for c in expected_columns if c in df.columns]
        df_clean = df.select(available_columns)
        
        # Add ingestion metadata
        df_clean = (
            df_clean
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_run_id", F.lit(self.run_id))
        )
        
        # Write to Delta in append mode for incremental processing.
        (
            df_clean
            .write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
        
        print(f"[BronzePipeline] Wrote {df_clean.count()} records to {output_path}")
        return output_path
    
    def run(self, inject_issues: bool = True) -> Dict[str, Any]:
        """
        Execute full Bronze pipeline: generate → validate → write.
        """
        # Step 1: Generate data
        df = self.generate_data(inject_issues=inject_issues)
        
        # Step 2: Validate
        validation = self.validate(df)
        
        # Step 3: Write to Delta
        output_path = self.write_to_delta(df, validation)
        
        # Step 4: Return summary
        return {
            "pipeline": "bronze",
            "run_id": self.run_id,
            "output_path": output_path,
            "record_count": df.count(),
            "validation": validation,
        }
