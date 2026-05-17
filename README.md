# Lakehouse Paradigm Comparison

> A production-grade, end-to-end data engineering portfolio project that benchmarks **Google BigQuery** and **Snowflake** as Lakehouse paradigms, built on a unified GCS-backed Medallion Architecture (Bronze → Silver → Gold).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#1-prerequisites)
  - [GCP Authentication](#2-authenticate-with-google-cloud)
  - [Provision Infrastructure (GCP)](#3-provision-gcp-infrastructure-terraform)
  - [Provision Infrastructure (Databricks)](#4-provision-databricks-secrets-terraform)
  - [Setup Local Kafka](#5-setup-local-kafka-docker)
  - [Setup Python Environment](#6-setup-python-environment)
  - [Run the Producer](#7-run-the-kafka-producer)
  - [Run the Consumer](#8-run-the-consumer-kafka--gcs-bronze)
- [Kafka Topics](#kafka-topics)
- [Roadmap](#roadmap)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│              Olist E-Commerce CSVs (8 Entities)                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Python Producer (EventEnvelope + DLQ)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    MESSAGE BROKER (Kafka)                           │
│     8 Topics (per entity) + 1 DLQ  │  Exactly-Once Semantics       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Python Consumer (Batching + Time Flush)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GCS BRONZE LAYER  (Raw, Append-Only)                   │
│   {entity}/year={Y}/month={M}/day={D}/batch_{uuid}.json (NDJSON)   │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Databricks (PySpark + Delta Lake)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GCS SILVER LAYER  (Cleaned, Deduplicated)              │
│                   Delta Lake format (.delta)                        │
└───────────┬───────────────────────────────────────┬─────────────────┘
            │ dbt (BigQuery adapter)                │ dbt (Snowflake adapter)
            ▼                                       ▼
┌────────────────────────┐             ┌────────────────────────────┐
│  BigQuery GOLD LAYER   │             │   Snowflake GOLD LAYER     │
│  ecommerce_gold.*      │             │   LAKEHOUSE_RAW.BRONZE.*   │
└────────────────────────┘             └────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| **Infrastructure** | Terraform | GCS Buckets, BigQuery, IAM, Databricks Secrets |
| **Secret Management** | Infisical Cloud | Centralized secrets (Kafka, GCP keys) |
| **Message Broker** | Apache Kafka (Docker) | Per-entity topic streaming with DLQ |
| **Ingestion** | Python + `confluent-kafka` | Producer (CSV→Kafka) & Consumer (Kafka→GCS) |
| **Processing** | Databricks (Serverless) + PySpark | Bronze→Silver transformation with Delta Lake |
| **Transformation** | dbt | Silver→Gold SQL models (BigQuery + Snowflake) |
| **Analytical Store** | Google BigQuery | Gold layer for GCP paradigm |
| **Analytical Store** | Snowflake | Gold layer for cross-cloud paradigm |
| **Package Manager** | `uv` + `hatchling` | Fast, modern Python dependency management |

---

## Project Structure

```
lakehouse-paradigm-comparison/
├── .gitignore
├── README.md
├── ROADMAP.md
├── docker-compose.yaml             # Kafka + Kafka-UI (local dev)
├── scripts/
│   ├── auth-setup.sh               # gcloud ADC authentication helper
│   └── setup-minikube.sh           # Kafka on local K8s (optional)
├── infrastructure/
│   ├── gcp/                        # Terraform: GCS, BigQuery, IAM
│   │   ├── providers.tf
│   │   ├── main.tf                 # GCS Buckets (Bronze, Silver, Gold)
│   │   ├── bigquery.tf             # BigQuery Dataset & Tables
│   │   ├── iam.tf                  # IAM roles for Snowflake & Databricks
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── terraform.tfvars        # ⚠️ Not committed to Git
│   │   └── terraform.tfvars.example
│   └── databricks/                 # Terraform: Databricks Secret Scope
│       ├── providers.tf
│       ├── secrets.tf              # Secret Scope + GCP SA key injection
│       ├── variables.tf
│       ├── outputs.tf
│       ├── terraform.tfvars        # ⚠️ Not committed to Git
│       └── terraform.tfvars.example
├── ingestion/                      # Python Ingestion Engine (uv project)
│   ├── .env                        # ⚠️ Not committed to Git
│   ├── .env.example                # Configuration template
│   ├── pyproject.toml              # Project metadata + dependencies
│   └── source/
│       ├── main.py                 # CLI entry point (Typer)
│       ├── config/
│       │   └── settings.py         # Pydantic BaseSettings
│       ├── controller/
│       │   ├── producer.py         # CLI: batch / single-file producer
│       │   └── consumer_to_gcs.py  # CLI: stream / stream-all consumer
│       ├── services/
│       │   ├── infisical_manager.py # Infisical SDK secret loader
│       │   ├── kafka_svc.py        # Kafka base client (confluent-kafka)
│       │   ├── kafka_producer_svc.py # CSV → EventEnvelope → Kafka
│       │   ├── kafka_consumer_svc.py # Kafka → GCS (batch + time flush)
│       │   └── storage_svc.py      # GCS / S3 / ADLS adapter (ABC)
│       ├── mapper/
│       │   └── configs/
│       │       └── topic_map.py    # DATASET_TOPIC_MAP (filename → topic)
│       ├── core/
│       │   ├── constants.py        # Enums: Topics, Layers
│       │   └── exceptions.py       # Custom pipeline exceptions
│       └── models/
│           └── event_envelope.py   # EventEnvelope data contract model
├── processing/
│   └── databricks/
│       └── bronze_to_silver.py     # PySpark: Bronze NDJSON → Silver Delta
├── transformation/
│   └── dbt_project/
│       ├── dbt_project.yml
│       ├── profiles.yml            # BigQuery + Snowflake targets
│       ├── models/
│       │   ├── staging/            # stg_orders, stg_customers, ...
│       │   ├── intermediate/       # int_orders_enriched, ...
│       │   └── marts/              # fct_orders, dim_customers, ...
│       ├── tests/
│       └── macros/
└── .github/
    └── workflows/
        ├── terraform-ci.yml        # Terraform fmt + validate on PR
        └── python-ci.yml           # Ruff lint + pytest on PR
```

---

## Getting Started

### 1. Prerequisites

| Tool | Version | Install |
|---|---|---|
| Google Cloud SDK | latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| Terraform | ≥ 1.5 | [developer.hashicorp.com/terraform](https://developer.hashicorp.com/terraform/downloads) |
| Python | ≥ 3.10 | [python.org](https://www.python.org/downloads/) |
| uv | latest | `pip install uv` |
| Docker Desktop | latest | [docker.com](https://www.docker.com/products/docker-desktop/) |

### 2. Authenticate with Google Cloud

```bash
gcloud auth login
gcloud auth application-default login
```

This sets up **Application Default Credentials (ADC)** used by both Terraform and the Python ingestion engine for keyless authentication.

### 3. Provision GCP Infrastructure (Terraform)

```bash
cd infrastructure/gcp
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set your project_id and region

terraform init
terraform plan
terraform apply
```

This provisions: GCS Buckets (bronze, silver, gold), BigQuery Dataset, and IAM roles.

### 4. Provision Databricks Secrets (Terraform)

```bash
cd infrastructure/databricks
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set databricks_host, token, and GCP SA key path

terraform init
terraform apply
```

This creates a `gcp_secrets` Secret Scope in your Databricks Workspace and injects the GCP Service Account key — no manual copy-paste required.

### 5. Setup Local Kafka (Docker)

```bash
# From the project root
docker compose up -d

# Verify Kafka is running
# Kafka Broker: localhost:9092
# Kafka UI:     http://localhost:8080
```

### 6. Setup Python Environment

```bash
cd ingestion
uv sync
```

Copy the environment template and fill in your credentials (Infisical Machine ID/Secret from your Infisical project):

```bash
cp .env.example .env
# Edit .env with your Infisical credentials
```

> **Note:** `GCS_BRONZE_BUCKET` in `.env` is auto-picked up by the consumer — no need to pass `--bucket` on every command.

### 7. Run the Kafka Producer

Produces all 8 Olist CSV datasets to their respective Kafka topics:

```bash
cd ingestion

# Batch mode: produce all CSVs from a directory
uv run ingestion-run producer batch --data-dir /path/to/olist/data --env dev

# Single file mode
uv run ingestion-run producer single-file --filename olist_orders_dataset.csv --data-dir /path/to/olist/data
```

### 8. Run the Consumer (Kafka → GCS Bronze)

**Local development** — stream all topics concurrently (one thread per topic):

```bash
uv run ingestion-run consumer-to-gcs stream-all --env dev
```

**Production / Kubernetes** — one isolated pod per topic for full observability:

```bash
# Orders
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.orders.v1 \
  --entity order \
  --group-id gcs-bronze-order-dev-v0.0.1 \
  --batch-size 1000

# Order Items
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.order-items.v1 \
  --entity order_item \
  --group-id gcs-bronze-order-item-dev-v0.0.1 \
  --batch-size 1000

# Payments
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.payments.v1 \
  --entity payment \
  --group-id gcs-bronze-payment-dev-v0.0.1 \
  --batch-size 1000

# Reviews
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.reviews.v1 \
  --entity review \
  --group-id gcs-bronze-review-dev-v0.0.1 \
  --batch-size 1000

# Customers
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.customers.v1 \
  --entity customer \
  --group-id gcs-bronze-customer-dev-v0.0.1 \
  --batch-size 1000

# Products
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.products.v1 \
  --entity product \
  --group-id gcs-bronze-product-dev-v0.0.1 \
  --batch-size 1000

# Product Categories
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.product-categories.v1 \
  --entity product_category \
  --group-id gcs-bronze-product-category-dev-v0.0.1 \
  --batch-size 1000

# Sellers
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.sellers.v1 \
  --entity seller \
  --group-id gcs-bronze-seller-dev-v0.0.1 \
  --batch-size 1000

# Geolocation
uv run ingestion-run consumer-to-gcs stream \
  --topic ecommerce.olist.geolocation.v1 \
  --entity geolocation \
  --group-id gcs-bronze-geolocation-dev-v0.0.1 \
  --batch-size 1000
```

> **Consumer Design:** Uploads are triggered when **either** the batch reaches `--batch-size` records **or** 30 seconds have elapsed (time-window flush), ensuring near-real-time latency without small-file explosion.

---

## Kafka Topics

| Topic | Entity | Group ID Pattern |
|---|---|---|
| `ecommerce.olist.orders.v1` | `order` | `gcs-bronze-order-{env}-{version}` |
| `ecommerce.olist.order-items.v1` | `order_item` | `gcs-bronze-order-item-{env}-{version}` |
| `ecommerce.olist.payments.v1` | `payment` | `gcs-bronze-payment-{env}-{version}` |
| `ecommerce.olist.reviews.v1` | `review` | `gcs-bronze-review-{env}-{version}` |
| `ecommerce.olist.customers.v1` | `customer` | `gcs-bronze-customer-{env}-{version}` |
| `ecommerce.olist.products.v1` | `product` | `gcs-bronze-product-{env}-{version}` |
| `ecommerce.olist.product-categories.v1` | `product_category` | `gcs-bronze-product-category-{env}-{version}` |
| `ecommerce.olist.sellers.v1` | `seller` | `gcs-bronze-seller-{env}-{version}` |
| `ecommerce.olist.geolocation.v1` | `geolocation` | `gcs-bronze-geolocation-{env}-{version}` |
| `ecommerce.dlq.v1` | *(Dead Letter Queue)* | — |

> **Group ID versioning:** Bumping the version suffix (e.g. `v0.0.1` → `v0.0.2`) resets the consumer offset, allowing full historical replay without touching Kubernetes manifests.

---

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for the full phased implementation plan.

| Phase | Description | Status |
|---|---|---|
| Phase 1 | GCP Identity & Security (Service Account, ADC) | ✅ Done |
| Phase 2 | Infrastructure as Code (Terraform: GCS, BigQuery) | ✅ Done |
| Phase 3 | Ingestion Engine (Kafka Producer + GCS Consumer) | ✅ Done |
| Phase 4 | Snowflake Configuration (GCS External Stage) | 🔄 In Progress |
| Phase 5 | Databricks Processing (PySpark Bronze→Silver) | 🔄 In Progress |
| Phase 6 | dbt Transformation (Silver→Gold, dual platform) | 🔲 Planned |
| Phase 7 | Benchmark & Documentation | 🔲 Planned |
| Phase 8 | Data Lineage, Governance & Observability | 🔲 Planned |
| Phase 9 | Multi-Cloud Extension (AWS S3 + Redshift, Azure ADLS) | 🔲 Planned |

---

## Design Decisions

### Why per-topic consumer pods (not one consumer for all)?
Each Kafka topic maps to one isolated consumer pod in Kubernetes. This provides:
- **Independent log streams** per entity (no mixed logs)
- **Individual resource limits & restart policies** per consumer
- **Granular scaling** — high-volume topics (orders) can scale independently

### Why batch-size + time-window flush?
Pure count-based batching leaves data stranded in memory during low-traffic periods. The dual-trigger strategy (`--batch-size` OR 30-second timeout) ensures:
- **Near-real-time latency** (max 30 seconds end-to-end)
- **No small-file problem** (files are always ≥ 1 record, optimally ~1000)

### Why Terraform for Databricks secrets?
The `infrastructure/databricks/` module treats Databricks Secret Scopes as Infrastructure as Code — reproducible, auditable, and GitOps-compliant. No manual UI clicks or ad-hoc scripts required in production.

### Why Bronze is append-only (no deduplication at GCS)?
Bronze is the **immutable raw history** of all events. Deduplication happens in the Silver layer via Spark `MERGE` (Delta Lake UPSERT), which is the standard Medallion Architecture pattern used at companies like Uber, Airbnb, and Gojek.