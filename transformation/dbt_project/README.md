# dbt Transformation Layer — E-Commerce Lakehouse

> Dual-paradigm dbt project transforming Silver Delta Lake tables into a production-ready Gold Layer on both **Google BigQuery** and **Snowflake** using a single shared codebase.

---

## Overview

This dbt project implements the **Silver → Gold** transformation layer of the Lakehouse Medallion Architecture. It supports two analytical warehouse targets from a single codebase, powered by a custom cross-warehouse macro.

| Target | Warehouse | Dataset / Schema | Materialization |
|---|---|---|---|
| `bigquery` | Google BigQuery | `ecommerce_gold` | `table` |
| `snowflake` | Snowflake | `LAKEHOUSE_RAW.GOLD` | `table` |

---

## Model Layers

### Staging (`models/staging/`)
Raw Silver Delta Lake data surfaced as clean, typed SQL views. One model per source entity.

| Model | Source Entity | Description |
|---|---|---|
| `stg_orders` | `order` | Order header records with timestamps and status |
| `stg_order_items` | `order_item` | Line-item details: product, seller, price, freight |
| `stg_payments` | `payment` | Payment method and installment data |
| `stg_reviews` | `review` | Customer review scores and comments |
| `stg_customers` | `customer` | Customer demographics and geolocation |
| `stg_products` | `product` | Product attributes and category |
| `stg_product_category_translation` | `product_category` | Portuguese → English category name mapping |
| `stg_sellers` | `seller` | Seller city and state details |
| `stg_geolocation` | `geolocation` | ZIP-code level geolocation coordinates |

### Intermediate (`models/intermediate/`)
Business logic enrichment and joins between staging models before final aggregation.

| Model | Description |
|---|---|
| `int_orders_enriched` | Orders joined with payment totals, review scores, and delivery delta |

### Marts (`models/marts/`)
Final analytical tables — the Gold Layer consumed by BI tools and dashboards.

| Model | Grain | Description |
|---|---|---|
| `fct_orders` | Order | Order-level fact with payment, review, delivery metrics |
| `fct_order_items` | Order Item | Line-item fact with price, freight, and total value |
| `dim_customers` | Customer | Customer dimension with lifetime value & review score |
| `dim_products` | Product | Product dimension with translated category name |
| `dim_sellers` | Seller | Seller dimension with city and state |
| `mart_category_performance` | Category | Revenue, orders, and avg price aggregated by category |
| `mart_seller_performance` | Seller | Revenue, freight, and order count aggregated by seller |
| `pipeline_audit_log` | Pipeline Run | ETL run metadata for observability (Grafana) |

---

## Cross-Warehouse Macro

The `col()` macro in `macros/col.sql` handles the fundamental difference in how BigQuery and Snowflake expose Delta Lake external tables:

```sql
-- BigQuery: columns are first-class
cast(field_name as STRING)

-- Snowflake: all data is inside a VARIANT column called VALUE
cast(value:field_name as STRING)
```

Usage in staging models:

```sql
select
    {{ col('order_id', 'STRING') }}       as order_id,
    {{ col('customer_id', 'STRING') }}    as customer_id,
    {{ col('order_status', 'STRING') }}   as order_status,
    {{ col('order_purchase_timestamp', 'TIMESTAMP') }} as order_purchase_timestamp
from {{ source('silver', 'order') }}
```

---

## Running the Project

### Prerequisites

```bash
# Install dependencies via uv (from repo root)
cd transformation/dbt_project
uv sync
```

### BigQuery

```bash
uv run dbt run --profiles-dir . --target bigquery
uv run dbt test --profiles-dir . --target bigquery
```

### Snowflake

```bash
# Set credentials
$env:SNOWFLAKE_ACCOUNT="<your-account>"
$env:SNOWFLAKE_USER="<your-user>"
$env:SNOWFLAKE_PASSWORD="<your-password>"

# Register external tables (first time only)
uv run dbt run-operation create_external_tables --profiles-dir . --target snowflake

# Run models & tests
uv run dbt run --profiles-dir . --target snowflake
uv run dbt test --profiles-dir . --target snowflake
```

### Via Infisical (No Manual Credentials)

```bash
uv run python run_dbt_infisical.py run --profiles-dir . --target snowflake
uv run python run_dbt_infisical.py test --profiles-dir . --target snowflake
```

### View Lineage DAG

```bash
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir . --port 8085
# Open: http://localhost:8085
```

---

## Data Quality Tests

Tests are defined in `models/staging/schema.yml` and `models/marts/schema.yml` and run via `dbt test`.

| Test Type | Applied To | Example |
|---|---|---|
| `not_null` | All primary and foreign keys | `order_id`, `customer_id` |
| `unique` | Primary keys | `order_id` in `fct_orders` |
| `relationships` | Foreign keys | `fct_order_items.order_id` → `fct_orders.order_id` |
| `accepted_values` | Status columns | `order_status` ∈ {`delivered`, `shipped`, ...} |

---

## Packages

| Package | Version | Purpose |
|---|---|---|
| `dbt-utils` | latest | Cross-warehouse utility macros |
| `elementary-data` | latest | Data observability & anomaly detection |

---

*Part of the [Lakehouse Paradigm Comparison](../../README.md) portfolio project.*
