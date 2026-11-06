output "catalog" {
  value = databricks_catalog.payments.name
}

output "jobs_cluster_policy_id" {
  description = "Pass to the bundle as cluster_policy_id."
  value       = databricks_cluster_policy.jobs.id
}
