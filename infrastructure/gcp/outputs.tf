output "gcs_bronze_uri" {
  value       = "gs://${var.project_id}-bronze"
  description = "GCS URI for Bronze layer"
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.dataset.dataset_id
  description = "BigQuery Dataset ID"
}
