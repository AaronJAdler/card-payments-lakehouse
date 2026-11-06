output "workspace_url" {
  description = "Pass to infra/databricks as workspace_url, and to the bundle as its host."
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "access_connector_id" {
  description = "Pass to infra/databricks as access_connector_id."
  value       = azurerm_databricks_access_connector.unity.id
}

output "lake_url" {
  description = "Pass to infra/databricks as lake_url."
  value       = "abfss://${azurerm_storage_container.lake.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}

output "landing_url" {
  description = "Pass to infra/databricks as landing_url, and to the bundle as landing_root."
  value       = "abfss://${azurerm_storage_container.landing.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}
