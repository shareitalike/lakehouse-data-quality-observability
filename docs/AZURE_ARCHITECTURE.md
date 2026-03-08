# Azure Lakehouse Data Quality Architecture

This document describes the enterprise architecture deployed for the Lakehouse Data Quality and Observability Framework on Microsoft Azure.

## 1. High-Level Architecture Diagram

```text
[ External Sources / APIs ] 
         │
         ▼
[ Azure Data Factory (ADF) ] ──(Triggers & Parameters)──┐
         │                                              │
         ▼ (Ingest)                                     ▼
[ ADLS Gen2 (Bronze) ] ───────────(Reads)──────── [ Azure Databricks ]
                                                        │
                                                        ├──(Validates & Transforms)
                                                        │
[ ADLS Gen2 (Silver/Gold) ] ◄─────(Writes)──────────────┘
[ Delta Metrics Table     ] 
         │
         ▼
[ Power BI Dashboards ]
```

## 2. Core Components

### Azure Data Factory (ADF) - The Orchestrator
ADF serves as the enterprise scheduler and metadata-driven orchestrator.
- **Trigger**: Runs pipelines on a schedule or event (e.g., file arrival in ADLS).
- **Execution**: Uses a `Databricks Notebook Activity` or `Databricks Python Activity` to trigger `pipelines/orchestrator.py`.
- **Parameter Injection**: Passes `--env azure`, `--pipeline-date`, and `--storage-account` dynamically at runtime.

### Azure Databricks - The Compute Engine
Databricks handles the heavy lifting of the Data Quality framework.
- **Job Clusters**: Executed on ephemeral Job Clusters to minimize cost.
- **Unity Catalog**: Tables (Bronze, Silver, Gold, and Observability) are registered in Unity Catalog for data governance and fine-grained access control.

### Azure Data Lake Storage Gen2 (ADLS Gen2) - The Storage Layer
ADLS Gen2 provides the underlying storage using the hierarchical namespace.
- **Protocol**: Accessed via the optimized `abfss://` protocol instead of legacy `wasbs://`.
- **Security**: Access is secured via Azure Active Directory (AAD) Service Principals, avoiding the need for insecure storage account keys.

### Azure Key Vault - Secrets Management
Key Vault securely stores:
- Service Principal Client IDs and Secrets.
- ADLS Gen2 Tenant IDs.
Databricks connects to Key Vault via a Secret Scope, making credentials invisible in the codebase.

## 3. Security & Authentication Pattern

The framework uses **OAuth 2.0 with a Service Principal** to authenticate Spark to ADLS Gen2. 

Instead of mounting the storage account (which is a deprecated Databricks pattern), the Spark session is injected with AAD credentials dynamically during initialization (`utils/path_resolver.py`). This is the recommended enterprise pattern.
