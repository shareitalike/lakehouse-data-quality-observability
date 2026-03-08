# Azure Databricks Enterprise Deployment Runbook

This runbook outlines the deployment and execution steps for the Lakehouse Data Quality framework within an Enterprise Azure Databricks environment.

---

## 🛠️ Step 1: Azure Environment Provisioning
The infrastructure for this project is managed via Terraform. Ensure the following resources are provisioned in your target environment (Dev/Staging/Prod):
1. **Azure Databricks Workspace** (Premium Tier for Role-Based Access Control).
2. **Azure Data Lake Storage Gen2 (ADLS Gen2)** with hierarchical namespace enabled.
3. **Azure Key Vault** for securely storing Service Principal credentials.

---

## 📦 Step 2: Code Deployment (Git Integration)
We strictly avoid manual code uploads in higher environments. Code is deployed via Databricks Repos integrated with our enterprise Git provider.

1. Navigate to **Workspace** -> **Repos**.
2. Click **Add Repo** and clone the repository using the Azure DevOps/GitHub service account token.
3. For production deployments, this step is automated via our CI/CD pipelines (e.g., GitHub Actions or Azure DevOps Pipelines) using the Databricks REST API.

---

## ⚙️ Step 3: Cluster Configuration
To run the orchestration pipelines, configure an Automated Job Cluster with the following specifications:

1. **Databricks Runtime:** 14.3 LTS (or higher).
2. **Node Type:** Standard_DS3_v2 (Auto-scaling: 2 to 8 workers depending on data volume SLAs).
3. **Advanced Options -> Spark Config:**
   Configure the Service Principal to allow Databricks to mount/read the ADLS Gen2 storage:
   ```text
   fs.azure.account.auth.type OAuth
   fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
   fs.azure.account.oauth2.client.id {{secrets/keyvault-scope/databricks-sp-client-id}}
   fs.azure.account.oauth2.client.secret {{secrets/keyvault-scope/databricks-sp-client-secret}}
   fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/{{tenant_id}}/oauth2/token
   ```
4. **Libraries:** Ensure `delta-spark` is installed if running on a runtime that requires explicit Delta upgrades.

---

## 📓 Step 4: Execution & Orchestration
This pipeline is designed to be triggered by **Azure Data Factory (ADF)** on a scheduled cadence (e.g., hourly or daily).

If executing manually for testing or debugging in the Dev environment:
1. Open the `pipelines/orchestrator.py` script.
2. In a Databricks Notebook, execute the entry point:

```python
# Add the repo to the system path
import sys
sys.path.append("/Workspace/Repos/data-engineering/lakehouse-dq-framework")

# Execute the pipeline
from pipelines.orchestrator import run_full_pipeline
from config.pipeline_configs import PipelineConfig

# Load environment-specific config
config = PipelineConfig()
config.environment = "azure"

results = run_full_pipeline(config, inject_issues=False)

print(f"Master Run ID: {results['master_run_id']}")
```

---

## 📊 Step 5: Data Quality Observability
Once the pipeline successfully executes, the Silver/Gold tables and the Quarantine/Metrics tables are updated in ADLS Gen2.

To visualize the data quality metrics for business stakeholders:
1. Connect **Databricks SQL** or **PowerBI** to the Databricks Workspace.
2. Query the observability tables directly to monitor anomaly rates and data freshness SLAs:
```sql
SELECT 
    execution_timestamp, 
    layer, 
    rule_id, 
    failure_rate 
FROM delta.`abfss://lakehouse@<storage_account>.dfs.core.windows.net/observability/metrics`
ORDER BY execution_timestamp DESC;
```
