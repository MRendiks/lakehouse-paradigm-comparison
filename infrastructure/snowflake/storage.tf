# ── Storage Integration (GCS ↔ Snowflake Handshake) ──────────────────────────
# This creates a Snowflake-owned GCP Service Account.
# Terraform then automatically grants that SA access to the GCS bucket below.
resource "snowflake_storage_integration" "gcs_integration" {
  name    = "GCS_INT"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "GCS"
  storage_allowed_locations = ["gcs://${var.gcs_bronze_bucket}/"]

  comment = "Integration for GCS Bronze bucket — used by Lakehouse pipeline"
}

# ── GCP IAM Handshake (Automated!) ───────────────────────────────────────────
# After creating the storage integration, Snowflake generates a GCP SA email.
# This resource reads that email and grants it Storage Object Viewer on the bucket.
# Previously this step was done MANUALLY in GCP Console — now it is fully automated!
resource "google_storage_bucket_iam_member" "snowflake_gcs_viewer" {
  bucket = var.gcs_bronze_bucket
  role   = "roles/storage.objectViewer"

  # This is the magic: we read the generated SA email directly from Snowflake's output
  member = "serviceAccount:${snowflake_storage_integration.gcs_integration.storage_gcp_service_account}"
}

# ── External Stage (GCS Bronze → Snowflake) ───────────────────────────────────
resource "snowflake_stage" "gcs_bronze_stage" {
  database            = snowflake_database.lakehouse.name
  schema              = snowflake_schema.bronze.name
  name                = "GCS_BRONZE_STAGE"
  url                 = "gcs://${var.gcs_bronze_bucket}/"
  storage_integration = snowflake_storage_integration.gcs_integration.name
  comment             = "External stage reading NDJSON batch files from GCS Bronze layer"

  # Explicitly depends on IAM — ensures GCS access is granted before stage is tested
  depends_on = [google_storage_bucket_iam_member.snowflake_gcs_viewer]
}
