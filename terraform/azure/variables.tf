variable "resource_group_name" {
  description = "The name of the resource group"
  type        = string
  default     = "rg-lakehouse-dq-prd"
}

variable "location" {
  description = "The Azure region to deploy to"
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account"
  type        = string
  default     = "dqlakehouseprdsa"
}

variable "databricks_workspace_name" {
  description = "Name of the Azure Databricks workspace"
  type        = string
  default     = "dbw-lakehouse-dq-prd"
}

variable "key_vault_name" {
  description = "Name of the Azure Key Vault"
  type        = string
  default     = "kv-lakehouse-dq-prd"
}

variable "data_factory_name" {
  description = "Name of the Azure Data Factory"
  type        = string
  default     = "adf-lakehouse-dq-prd"
}
