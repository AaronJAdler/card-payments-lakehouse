variable "prefix" {
  description = "Short name used in every resource name."
  type        = string
  default     = "paylake"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,10}$", var.prefix))
    error_message = "Use 3 to 11 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "environment" {
  description = "dev or prod. Only prod turns on Key Vault purge protection."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "location" {
  description = "Azure region. West US 2 is the closest Azure region to Sacramento."
  type        = string
  default     = "westus2"
}

variable "storage_replication" {
  description = "LRS is enough for a rebuildable demo. Use ZRS or GZRS for real data."
  type        = string
  default     = "LRS"
}

variable "landing_retention_days" {
  description = "Days to keep raw landing files before deletion."
  type        = number
  default     = 90
}

variable "monthly_budget_usd" {
  description = "Monthly spend that triggers budget alerts."
  type        = number
  default     = 50
}

variable "budget_start_date" {
  description = "First day of the budget period, in RFC 3339 format, on the first of a month."
  type        = string
  default     = "2026-11-01T00:00:00Z"
}

variable "alert_email" {
  description = "Where budget alerts go."
  type        = string
}
