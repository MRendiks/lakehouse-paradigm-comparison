output "gcs_bronze_uri" {
  value       = "gs://${var.project_id}-bronze"
  description = "GCS URI for Bronze layer"
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.dataset.dataset_id
  description = "BigQuery Dataset ID"
}

output "grafana_sa_key_json" {
  value       = base64decode(google_service_account_key.grafana_key.private_key)
  description = "Grafana Service Account Private Key (JSON format)"
  sensitive   = true
}
