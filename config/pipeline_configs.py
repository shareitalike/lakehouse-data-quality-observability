"""
Pipeline-level configuration.

This module centralizes all pipeline tuning parameters, enabling consistent 
management of pipeline behavior, environment settings, and data processing 
thresholds.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
from utils.path_resolver import LakehousePaths, PATHS


@dataclass
class QuarantineConfig:
    """
    Controls quarantine behavior.
    
    """
    enabled: bool = True
    max_quarantine_pct: float = 10.0
    
    alert_on_quarantine: bool = True
    include_failure_reason: bool = True
    max_records_to_store: int = 10000
    
    def __post_init__(self):
        if self.max_quarantine_pct < 0 or self.max_quarantine_pct > 100:
            raise ValueError("max_quarantine_pct must be between 0 and 100")


@dataclass
class SLAConfig:
    """
    Service Level Agreement thresholds.
    
    """
    bronze_freshness_hours: float = 24.0
    silver_freshness_hours: float = 4.0
    gold_freshness_hours: float = 2.0
    max_pipeline_duration_minutes: float = 60.0


@dataclass
class PipelineConfig:
    """
    Master pipeline configuration. Base implementation is local.
    """
    # Environment identity
    environment: str = "local"
    
    # Azure-specific storage configuration (used when environment == 'azure')
    azure_storage_account: Optional[str] = None
    azure_container: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id_secret_scope: Optional[str] = "keyvault-scope"
    azure_client_id_secret_key: Optional[str] = "databricks-sp-client-id"
    azure_client_secret_key: Optional[str] = "databricks-sp-client-secret"
    
    # Paths

    paths: LakehousePaths = field(default_factory=lambda: PATHS)
    
    # Quarantine settings
    quarantine: QuarantineConfig = field(default_factory=QuarantineConfig)
    
    # SLA settings
    sla: SLAConfig = field(default_factory=SLAConfig)
    
    # Pipeline identity
    pipeline_name: str = "lakehouse_dq_pipeline"
    pipeline_version: str = "1.0"
    
    # Watermark for incremental processing
    pipeline_date: Optional[str] = None
    
    # Data generation settings
    num_records: int = 10000
    
    random_seed: int = 42
    
    # Validation behavior
    fail_pipeline_on_critical: bool = False
    
    enable_schema_evolution: bool = True
    
    # Observability settings
    enable_observability: bool = True
    metrics_retention_days: int = 90
    
    @classmethod
    def default(cls) -> "PipelineConfig":
        """Create default configuration."""
        return cls()
    
    @classmethod
    def for_testing(cls) -> "PipelineConfig":
        """
        Configuration optimized for unit tests.
        
        """
        return cls(
            num_records=1000,
            quarantine=QuarantineConfig(
                max_quarantine_pct=50.0,  # Relaxed for test data
                max_records_to_store=100,
            ),
            sla=SLAConfig(
                bronze_freshness_hours=9999.0,  # Disable freshness in tests
                silver_freshness_hours=9999.0,
                gold_freshness_hours=9999.0,
            ),
        )
