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

resource "google_bigquery_table" "pipeline_audit_log" {
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "pipeline_audit_log"
  deletion_protection = false

  schema = <<EOF
[
  {
    "name": "run_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Unique identifier for the entire pipeline run"
  },
  {
    "name": "pipeline_stage",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Stage name: 'ingestion' | 'spark' | 'dbt'"
  },
  {
    "name": "entity_type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Name of the entity/dataset being processed, e.g., 'orders'"
  },
  {
    "name": "source_topic",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Kafka topic source if applicable (ingestion stage)"
  },
  {
    "name": "rows_processed",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Count of successfully processed/written rows"
  },
  {
    "name": "rows_failed",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Count of failed rows"
  },
  {
    "name": "started_at",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Start timestamp of the execution"
  },
  {
    "name": "finished_at",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "End timestamp of the execution"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Status of the execution: 'success' | 'failed' | 'partial'"
  },
  {
    "name": "error_message",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Detailed error message if the stage failed"
  }
]
EOF
}
