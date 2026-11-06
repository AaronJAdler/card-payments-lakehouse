variable "workspace_url" {
  description = "workspace_url output from infra/azure."
  type        = string
}

variable "access_connector_id" {
  description = "access_connector_id output from infra/azure."
  type        = string
}

variable "lake_url" {
  description = "lake_url output from infra/azure. Must end with a slash."
  type        = string
}

variable "landing_url" {
  description = "landing_url output from infra/azure."
  type        = string
}

variable "catalog" {
  description = "Unity Catalog catalog name."
  type        = string
  default     = "payments"
}

variable "engineers_group" {
  description = "Account-level group that builds and runs the pipeline. It must already exist."
  type        = string
  default     = "payments_engineers"
}

variable "analysts_group" {
  description = "Account-level group that reads gold tables. It must already exist."
  type        = string
  default     = "payments_analysts"
}

variable "spark_version" {
  description = "Databricks Runtime 17.3 LTS: Spark 4.0, Delta Lake 4.0, Python 3.12."
  type        = string
  default     = "17.3.x-scala2.13"
}

variable "allowed_node_types" {
  description = "VM sizes job clusters may use. The first is the default."
  type        = list(string)
  default     = ["Standard_D4ds_v5", "Standard_D8ds_v5", "Standard_E8ds_v5"]
}

variable "max_workers" {
  description = "Upper limit on workers for any job cluster."
  type        = number
  default     = 4
}
