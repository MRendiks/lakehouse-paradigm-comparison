# ── Snowflake Auth ────────────────────────────────────────────────────────────
variable "snowflake_organization" {
  description = "Snowflake organization name (from: SELECT CURRENT_ORGANIZATION_NAME())"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (from: SELECT CURRENT_ACCOUNT_NAME())"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username with ACCOUNTADMIN role"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake user password"
  type        = string
  sensitive   = true
}

# ── GCP Config ────────────────────────────────────────────────────────────────
variable "gcp_project_id" {
  description = "GCP Project ID (used for IAM handshake)"
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

# ── Snowflake Warehouse ───────────────────────────────────────────────────────
variable "warehouse_size" {
  description = "Snowflake Virtual Warehouse size"
  type        = string
  default     = "X-SMALL"
}

variable "warehouse_auto_suspend" {
  description = "Seconds of inactivity before warehouse suspends (cost control)"
  type        = number
  default     = 60
}
