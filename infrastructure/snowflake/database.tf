# ── Database ──────────────────────────────────────────────────────────────────
resource "snowflake_database" "lakehouse" {
  name    = "LAKEHOUSE_RAW"
  comment = "Lakehouse paradigm comparison — raw & bronze layer access"
}

# ── Schemas ───────────────────────────────────────────────────────────────────
resource "snowflake_schema" "bronze" {
  database = snowflake_database.lakehouse.name
  name     = "BRONZE"
  comment  = "External stage pointing to GCS Bronze bucket (NDJSON files)"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.lakehouse.name
  name     = "SILVER"
  comment  = "Cleaned & deduplicated data (read from Delta Lake via Databricks)"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.lakehouse.name
  name     = "GOLD"
  comment  = "Business-ready aggregated tables (populated by dbt)"
}

# ── Virtual Warehouse ─────────────────────────────────────────────────────────
# X-SMALL is sufficient for portfolio/development workloads.
# auto_suspend = 60s ensures minimal credit consumption.
resource "snowflake_warehouse" "lakehouse_wh" {
  name           = "LAKEHOUSE_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.warehouse_auto_suspend
  auto_resume    = true
  comment        = "Warehouse for lakehouse paradigm comparison queries"
}
