output "snowflake_database" {
  description = "Snowflake database name"
  value       = snowflake_database.lakehouse.name
}

output "snowflake_warehouse" {
  description = "Snowflake virtual warehouse name"
  value       = snowflake_warehouse.lakehouse_wh.name
}

output "snowflake_stage_name" {
  description = "External stage name for querying from Snowflake"
  value       = "@${snowflake_database.lakehouse.name}.${snowflake_schema.bronze.name}.${snowflake_stage.gcs_bronze_stage.name}"
}

output "snowflake_gcp_service_account" {
  description = "Snowflake-owned GCP SA email (auto-granted Storage Object Viewer via Terraform)"
  value       = snowflake_storage_integration.gcs_integration.storage_gcp_service_account
}

output "verify_stage_query" {
  description = "Run this query in Snowflake to verify GCS files are accessible"
  value       = "LIST @${snowflake_database.lakehouse.name}.${snowflake_schema.bronze.name}.${snowflake_stage.gcs_bronze_stage.name};"
}
