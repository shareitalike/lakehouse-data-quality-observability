# Lakehouse Data Quality + Observability Framework

## Overview

A **production-grade, configuration-driven Data Quality + Observability framework** for Medallion architecture pipelines (Bronze → Silver → Gold), built entirely with PySpark DataFrame API and Delta Lake.

This framework demonstrates:
- **Lakehouse maturity**: Medallion enforcement, schema contracts, and Delta transaction reasoning
- **Production-ready architecture**: data contracts, rule versioning, and quarantine patterns
- **Professional design**: every design decision is documented and architecturally sound

## Tech Stack

| Component | Technology |
|---|---|
| Processing Engine | Azure Databricks (PySpark) / Local Spark |
| Storage Format | Delta Lake |
| Storage Layer | Azure Data Lake Storage Gen2 (ADLS Gen2) / Local |
| Orchestration | Azure Data Factory (ADF) Ready |
| Infrastructure | Terraform (IaC) |
| Rule Configuration | Python Dataclasses |
| Validation Queries | SQL (dbt-style philosophy) |
| Metrics Storage | Delta Table (append-only) |

## Enterprise Azure Integration

This project includes production-ready Azure integrations to demonstrate enterprise consulting standards:
- **ADLS Gen2 `abfss://` Paths**: Native hierarchical namespace support.
- **Service Principal Authentication**: Boilerplate OAuth 2.0 configuration for secure Databricks-to-ADLS connectivity via Azure Key Vault.
- **ADF Parameterization**: Pipeline orchestrator accepts dynamic arguments (`--env azure`, `--pipeline-date`) for seamless Azure Data Factory scheduling.
- **Infrastructure as Code**: Terraform module provided for deploying the Resource Group, ADLS Gen2, Databricks Workspace, and Key Vault.

## Quick Start

### Prerequisites
```bash
pip install pyspark delta-spark
```

### Run the Full Pipeline
```python
from pipelines.orchestrator import run_full_pipeline
from config.pipeline_configs import PipelineConfig

config = PipelineConfig.default()
results = run_full_pipeline(config)
```

### Run Individual Checks
```python
from rules.not_null_check import not_null_check
from config.rule_configs import NotNullRule

rule = NotNullRule(
    rule_id="bronze_customer_id_not_null",
    rule_version="1.0",
    column_name="customer_id",
    severity="critical",
    layer="bronze"
)
result = not_null_check(df, rule)
print(f"Passed: {result.passed}, Failure Rate: {result.failure_rate:.2%}")
```

## Project Structure

```
lakehouse_dq_framework/
├── config/           # Dataclass-based configurations
├── data_generation/  # Synthetic data + fault injection
├── rules/            # 10 validation rule functions
├── engine/           # Validation orchestration + quarantine
├── observability/    # Metrics collection + Delta store
├── pipelines/        # Bronze → Silver → Gold pipelines
├── schemas/          # StructType definitions per layer
├── tests/            # Unit + integration tests
└── docs/             # Architecture, guides, interview prep
```

## Architecture Overview

The framework is designed with modularity and scalability in mind:
- **Modular Rules**: Validation logic is decoupled from orchestration.
- **Config-Driven**: Rule definitions are stored as data, allowing for easy updates without code changes.
- **Automated Quarantine**: Failed records are automatically routed to specialized Delta tables for investigation.
- **Observability Store**: All validation metrics are persisted for historical analysis and trend detection.

## Key Design Decisions

1. **Config-driven validation**: Rules are data, not code. Adding a new check = adding a dataclass instance, not writing a new function.
2. **Quarantine over failure**: Bad records route to quarantine tables, never crash the pipeline.
3. **Observability ≠ Validation**: Validation gates data. Observability tracks trends. Different concerns, different tables.
4. **Append-only metrics**: Observability table is immutable — enables time-series trend analysis and audit trails.
5. **Environment-agnostic paths**: Auto-detects Databricks vs local Spark for zero-config portability.

## Documentation

- [Azure Architecture](docs/AZURE_ARCHITECTURE.md)
- [Architecture Diagram](docs/architecture_diagram.md)
- [Scale Reasoning](docs/scale_reasoning.md)
- [Anti-Patterns](docs/anti_patterns.md)
- [Oracle DBA Mapping](docs/oracle_dba_mapping.md)
- [Interview Learning Guide](docs/data_quality_learning_guide.md)
