# Azure resources for the lakehouse. Apply this first; infra/databricks configures the workspace.
#
#   az login
#   terraform init && terraform apply -var alert_email=you@example.com
#   terraform destroy   # removes everything, including data

terraform {
  required_version = ">= 1.9"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.30"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
  # Shared keys are disabled on the storage account, so Terraform must use Entra ID for data calls.
  storage_use_azuread = true
}

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

locals {
  name = "${var.prefix}-${var.environment}"
  # Storage account and Key Vault names must be globally unique and alphanumeric.
  compact = "${replace(var.prefix, "-", "")}${var.environment}${random_string.suffix.result}"
  tags = {
    project     = "payments-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# ---------- Storage ----------

resource "azurerm_storage_account" "lake" {
  name                            = "st${local.compact}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = var.storage_replication
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # ADLS Gen2: real directories and atomic renames
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  shared_access_key_enabled       = false # every caller authenticates with Entra ID
  allow_nested_items_to_be_public = false
  tags                            = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }
}

# Upstream files land here. The catalog's managed tables live in the lake container.
resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "lake" {
  name                  = "lake"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Raw landing files are only needed for replays, so they move to cheaper tiers as they age.
resource "azurerm_storage_management_policy" "landing" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "age-out-landing-files"
    enabled = true
    filters {
      prefix_match = ["landing/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = var.landing_retention_days
      }
    }
  }
}

# ---------- Identity for Unity Catalog ----------

# A managed identity, so no secret or key is ever stored or rotated.
resource "azurerm_databricks_access_connector" "unity" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "unity_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
}

# Lets the person running Terraform upload sample files and inspect the lake.
resource "azurerm_role_assignment" "deployer_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------- Workspace ----------

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium" # Unity Catalog, column masks and row filters need Premium
  managed_resource_group_name = "rg-${local.name}-databricks-managed"
  tags                        = local.tags

  custom_parameters {
    no_public_ip = true # secure cluster connectivity: cluster nodes get no public IP addresses
  }
}

# ---------- Secrets ----------

resource "azurerm_key_vault" "main" {
  name                       = "kv${substr(local.compact, 0, 22)}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = var.environment == "prod"
  soft_delete_retention_days = 7
  tags                       = local.tags
}

resource "azurerm_role_assignment" "deployer_secrets" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------- Cost guardrail ----------

# Alerts only. Azure budgets never stop resources, so job clusters must also auto-terminate.
resource "azurerm_consumption_budget_resource_group" "monthly" {
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.main.id
  amount            = var.monthly_budget_usd
  time_grain        = "Monthly"

  time_period {
    start_date = var.budget_start_date
  }

  notification {
    enabled        = true
    threshold      = 50
    threshold_type = "Actual"
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 90
    threshold_type = "Actual"
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.alert_email]
  }

  notification {
    enabled        = true
    threshold      = 100
    threshold_type = "Forecasted"
    operator       = "GreaterThanOrEqualTo"
    contact_emails = [var.alert_email]
  }
}
