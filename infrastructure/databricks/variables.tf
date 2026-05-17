variable "databricks_host" {
  description = "Databricks Workspace URL (e.g. https://adb-xxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks Personal Access Token (PAT)"
  type        = string
  sensitive   = true
}

variable "gcp_sa_key_path" {
  description = "Absolute path to GCP Service Account JSON key file for Databricks"
  type        = string
}

variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "gcs_bronze_bucket" {
  description = "GCS Bronze bucket name (without gs:// prefix)"
  type        = string
}

variable "gcs_silver_bucket" {
  description = "GCS Silver bucket name (without gs:// prefix)"
  type        = string
}
