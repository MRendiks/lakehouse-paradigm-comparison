output "secret_scope_name" {
  description = "Name of the Databricks secret scope"
  value       = databricks_secret_scope.gcp.name
}

output "secret_keys" {
  description = "List of secret keys registered in the scope"
  value = [
    databricks_secret.gcp_sa_key.key,
    databricks_secret.gcp_project_id.key,
    databricks_secret.gcs_bronze_bucket.key,
    databricks_secret.gcs_silver_bucket.key,
  ]
}
