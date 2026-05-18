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

# --- GRAFANA MONITORING ---
resource "google_service_account" "grafana_sa" {
  account_id   = "grafana-sa"
  display_name = "Grafana Monitoring Service Account"
}

# 1. Role untuk membaca data di BigQuery
resource "google_project_iam_member" "grafana_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.grafana_sa.email}"
}

# 2. Role untuk menjalankan Query Jobs di BigQuery
resource "google_project_iam_member" "grafana_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.grafana_sa.email}"
}

# 3. Generate Service Account Key secara otomatis
resource "google_service_account_key" "grafana_key" {
  service_account_id = google_service_account.grafana_sa.name
}
