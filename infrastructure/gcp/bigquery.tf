resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "ecommerce_gold"
  friendly_name               = "Ecommerce Gold Dataset"
  description                 = "Dataset for gold layer data (managed by dbt)"
  location                    = var.region
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "sales_aggregate"
  deletion_protection = false

  schema = <<EOF
[
  {
    "name": "id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "amount",
    "type": "NUMERIC",
    "mode": "NULLABLE"
  }
]
EOF
}

# ── Silver Dataset (External Tables over Delta Lake) ──────────────────────────
resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id                  = "ecommerce_silver"
  friendly_name               = "Ecommerce Silver Dataset"
  description                 = "External tables pointing to GCS Delta Lake (Silver)"
  location                    = var.region
}

locals {
  entities = [
    "order",
    "order_item",
    "payment",
    "review",
    "customer",
    "product",
    "product_category",
    "seller",
    "geolocation"
  ]
}

resource "google_bigquery_table" "silver_tables" {
  for_each   = toset(local.entities)
  dataset_id = google_bigquery_dataset.silver_dataset.dataset_id
  table_id   = each.key
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "DELTA_LAKE"
    source_uris   = ["gs://silver-processed-project-aaa919f1-4345-401b-860/${each.key}"]
  }
}
