terraform {
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstatelakehouseprod"
    container_name       = "tfstate"
    key                  = "lakehouse-dq-framework.tfstate"
  }
}
