resource "google_storage_bucket" "bronze_layer" {
  name                        = "bronze-raw-${var.project_id}"
  location                    = "asia-southeast2"
  force_destroy               = true
  uniform_bucket_level_access = true
}