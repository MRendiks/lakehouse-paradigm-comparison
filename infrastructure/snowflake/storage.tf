# ── Storage Integration (GCS ↔ Snowflake Handshake) ──────────────────────────
# This creates a Snowflake-owned GCP Service Account.
# Terraform then automatically grants that SA access to the GCS bucket below.
resource "snowflake_storage_integration" "gcs_integration" {
  name    = "GCS_INT"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "GCS"
  storage_allowed_locations = [
    "gcs://${var.gcs_bronze_bucket}/",
    "gcs://${var.gcs_silver_bucket}/"
  ]

  comment = "Integration for GCS Bronze & Silver buckets — used by Lakehouse pipeline"
}

# ── GCP IAM Handshake (Automated!) ───────────────────────────────────────────
resource "google_storage_bucket_iam_member" "snowflake_gcs_viewer" {
  bucket = var.gcs_bronze_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${snowflake_storage_integration.gcs_integration.storage_gcp_service_account}"
}

resource "google_storage_bucket_iam_member" "snowflake_gcs_viewer_silver" {
  bucket = var.gcs_silver_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${snowflake_storage_integration.gcs_integration.storage_gcp_service_account}"
}

# ── External Stages ───────────────────────────────────────────────────────────
resource "snowflake_stage" "gcs_bronze_stage" {
  database            = snowflake_database.lakehouse.name
  schema              = snowflake_schema.bronze.name
  name                = "GCS_BRONZE_STAGE"
  url                 = "gcs://${var.gcs_bronze_bucket}/"
  storage_integration = snowflake_storage_integration.gcs_integration.name
  comment             = "External stage reading NDJSON batch files from GCS Bronze layer"
  depends_on          = [google_storage_bucket_iam_member.snowflake_gcs_viewer]
}

resource "snowflake_stage" "gcs_silver_stage" {
  database            = snowflake_database.lakehouse.name
  schema              = snowflake_schema.silver.name
  name                = "GCS_SILVER_STAGE"
  url                 = "gcs://${var.gcs_silver_bucket}/"
  storage_integration = snowflake_storage_integration.gcs_integration.name
  comment             = "External stage reading Delta Lake files from GCS Silver layer"
  depends_on          = [google_storage_bucket_iam_member.snowflake_gcs_viewer_silver]
}
