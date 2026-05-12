resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "ecommerce_gold"
  friendly_name               = "Ecommerce Gold Dataset"
  description                 = "Dataset for gold layer data"
  location                    = "US"
  default_table_expiration_ms = 3600000
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "sales_aggregate"

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
