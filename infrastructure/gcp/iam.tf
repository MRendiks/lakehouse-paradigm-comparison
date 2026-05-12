resource "google_service_account" "databricks_sa" {
  account_id   = "databricks-sa"
  display_name = "Databricks Service Account"
}

resource "google_service_account" "snowflake_sa" {
  account_id   = "snowflake-sa"
  display_name = "Snowflake Service Account"
}

# Role Binding for Databricks to access GCS
resource "google_project_iam_member" "databricks_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.databricks_sa.email}"
}

# Role Binding for Snowflake to access BigQuery/GCS
resource "google_project_iam_member" "snowflake_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.snowflake_sa.email}"
}
