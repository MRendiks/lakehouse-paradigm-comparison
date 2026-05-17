# ── Secret Scope ─────────────────────────────────────────────────────────────
# Satu scope sebagai "brankas" untuk semua secret GCP di Databricks.
# Setara dengan satu "project" di Infisical.
resource "databricks_secret_scope" "gcp" {
  name = "gcp_secrets"
}

# ── GCP Service Account Key ───────────────────────────────────────────────────
# Baca isi file JSON dari path yang didefinisikan di tfvars.
# File JSON ini TIDAK pernah di-commit ke Git — hanya dibaca saat terraform apply.
resource "databricks_secret" "gcp_sa_key" {
  scope        = databricks_secret_scope.gcp.name
  key          = "gcp_sa_key"
  string_value = file(pathexpand(var.gcp_sa_key_path))
}

# ── GCP Project ID ────────────────────────────────────────────────────────────
resource "databricks_secret" "gcp_project_id" {
  scope        = databricks_secret_scope.gcp.name
  key          = "gcp_project_id"
  string_value = var.gcp_project_id
}

# ── GCS Bucket Names ──────────────────────────────────────────────────────────
resource "databricks_secret" "gcs_bronze_bucket" {
  scope        = databricks_secret_scope.gcp.name
  key          = "gcs_bronze_bucket"
  string_value = var.gcs_bronze_bucket
}

resource "databricks_secret" "gcs_silver_bucket" {
  scope        = databricks_secret_scope.gcp.name
  key          = "gcs_silver_bucket"
  string_value = var.gcs_silver_bucket
}
