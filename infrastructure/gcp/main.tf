resource "google_storage_bucket" "bronze_layer" {
  name                        = "bronze-raw-${var.project_id}"
  location                    = "asia-southeast2"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "silver_layer" {
  name                        = "silver-processed-${var.project_id}"
  location                    = "asia-southeast2"
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "gold_layer" {
  name                        = "gold-serving-${var.project_id}"
  location                    = "asia-southeast2"
  force_destroy               = true
  uniform_bucket_level_access = true
}