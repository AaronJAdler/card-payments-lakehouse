# Unity Catalog objects and a cost policy inside the workspace that infra/azure created.
# A separate root module, because the Databricks provider needs that workspace to exist first.
#
#   terraform init && terraform apply -var-file=from-azure.tfvars

terraform {
  required_version = ">= 1.9"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.80"
    }
  }
}

# Uses your `az login` session. In CI, use a service principal with OIDC instead.
provider "databricks" {
  host      = var.workspace_url
  auth_type = "azure-cli"
}

# ---------- Storage access ----------

resource "databricks_storage_credential" "lake" {
  name    = "${var.catalog}-lake"
  comment = "Managed identity of the access connector. No secret is stored."
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "lake" {
  name            = "${var.catalog}-lake"
  url             = var.lake_url
  credential_name = databricks_storage_credential.lake.name
  comment         = "Managed storage for the payments catalog."
}

resource "databricks_external_location" "landing" {
  name            = "${var.catalog}-landing"
  url             = var.landing_url
  credential_name = databricks_storage_credential.lake.name
  read_only       = false # the generator job writes synthetic source files here
  comment         = "Raw files from the upstream system."
}

# ---------- Catalog and schemas ----------

resource "databricks_catalog" "payments" {
  name         = var.catalog
  storage_root = "${var.lake_url}${var.catalog}"
  comment      = "Synthetic card payments lakehouse."
  depends_on   = [databricks_external_location.lake]
}

locals {
  schemas = {
    bronze       = "Raw records exactly as they arrived, with arrival metadata."
    silver       = "Validated, deduplicated and conformed records."
    gold         = "Published facts and aggregates for analysts."
    gold_staging = "Gold candidates. Published to gold only after quality checks pass."
  }
}

resource "databricks_schema" "layer" {
  for_each     = local.schemas
  catalog_name = databricks_catalog.payments.name
  name         = each.key
  comment      = each.value
}

# ---------- Access ----------

# Engineers own the catalog. Analysts read gold only, never bronze, silver, or staging.
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.payments.name

  grant {
    principal  = var.engineers_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.analysts_group
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "gold" {
  schema = databricks_schema.layer["gold"].id

  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "landing" {
  external_location = databricks_external_location.landing.id

  grant {
    principal  = var.engineers_group
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

# ---------- Cost control ----------

# Every job cluster is forced to be small, short-lived and mostly spot capacity.
resource "databricks_cluster_policy" "jobs" {
  name = "${var.catalog}-jobs"
  definition = jsonencode({
    "cluster_type" : { "type" : "fixed", "value" : "job" },
    "spark_version" : { "type" : "fixed", "value" : var.spark_version },
    "node_type_id" : { "type" : "allowlist", "values" : var.allowed_node_types, "defaultValue" : var.allowed_node_types[0] },
    "num_workers" : { "type" : "range", "minValue" : 1, "maxValue" : var.max_workers, "defaultValue" : 2 },
    "azure_attributes.availability" : { "type" : "fixed", "value" : "SPOT_WITH_FALLBACK_AZURE" },
    "azure_attributes.first_on_demand" : { "type" : "fixed", "value" : 1 },
    "data_security_mode" : { "type" : "fixed", "value" : "SINGLE_USER" },
    "custom_tags.project" : { "type" : "fixed", "value" : "payments-lakehouse" }
  })
}

resource "databricks_permissions" "jobs_policy" {
  cluster_policy_id = databricks_cluster_policy.jobs.id

  access_control {
    group_name       = var.engineers_group
    permission_level = "CAN_USE"
  }
}
