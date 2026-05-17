# 🗺️ Lakehouse Paradigm Comparison — Detailed Roadmap

---

## ✅ Fase 1: Identity & Security *(DONE)*

Fondasi keamanan sebelum menyentuh tools apapun.

1. **Buat GCP Project** di GCP Console.
2. **Buat Service Account (SA)** bernama `cloud-migrator` dengan role `Editor`.
3. **IAM Binding:** Berikan email Gmail pribadimu role `Service Account Token Creator` pada SA tersebut agar bisa *impersonate* SA tanpa file JSON.
4. **Local Auth:** Jalankan `gcloud auth application-default login` di terminal laptop (ADC siap).

---

## ✅ Fase 2: Infrastructure as Code (Terraform) *(DONE)*

Otomatisasi pembuatan "wadah" data di GCP.

1. **Setup `providers.tf`** dengan metode **Impersonation** (tanpa file JSON).
2. **Buat `main.tf`** untuk men-deploy:
   - **GCS Bucket `bronze`** — Landing zone data mentah dari Kafka.
   - **GCS Bucket `silver`** — Data bersih hasil transformasi Spark.
   - **GCS Bucket `gold`** — Data agregat siap konsumsi BI.
   - **BigQuery Dataset `ecommerce_gold`** — Target Lakehouse versi Google.
3. **Setup `variables.tf` & `terraform.tfvars`** untuk manajemen variabel.
4. **Jalankan:** `terraform init` → `terraform plan` → `terraform apply`.

---

## 🔄 Fase 3: Ingestion Engine (Python & Kafka) *(DONE)*

Membangun pipeline untuk memindahkan data dari sumber ke GCS.

### 3.1 — Project Setup ✅
- [x] Inisialisasi folder `ingestion/` dengan *Clean Architecture* (`source/config`, `source/controller`, `source/services`).
- [x] Setup `pyproject.toml` modern dengan `uv`, `hatchling`, dan `ruff`.
- [x] Integrasikan **Infisical Cloud** sebagai *Secret Manager* (tanpa self-hosted DB).
  - [x] Buat `source/config/settings.py` — Pydantic `BaseSettings` sebagai wadah konfigurasi.
  - [x] Buat `source/services/infisical_manager.py` — `InfisicalManager` class untuk menarik semua *secrets* dari Infisical.
- [x] Nama & versi CLI diambil otomatis dari `pyproject.toml` via `importlib.metadata`.
- [x] ADC (`gcloud auth application-default login`) berhasil dijalankan untuk akses GCS lokal.

### 3.2 — Kafka Producer 🔲
- [x] Jalankan Kafka & Kafka-UI via **Docker Compose** (sudah ada di `docker-compose.yaml`).
- [x] Implementasi `EventEnvelope` model — setiap message dibungkus metadata standar (`event_id`, `event_type`, `schema_version`, `checksum`).
- [x] Gunakan `DATASET_TOPIC_MAP` dari `constants.py` untuk routing otomatis ke topic yang tepat berdasarkan nama file CSV.
- [x] Buat `source/services/kafka_producer.py` — CSV reader → `EventEnvelope.create()` → `KafkaService.produce()`.
- [x] Buat `source/controller/ingestion_ctl.py` — command CLI `engine` yang memanggil producer untuk semua dataset.
- [x] Test end-to-end: Pastikan semua **8 topic** terlihat di **Kafka-UI** (`localhost:8080`) dengan pesan terkirim.
- [x] Verifikasi DLQ (`ecommerce.dlq.v1`) menerima message ketika produce gagal.

### 3.3 — Kafka Consumer → GCS (Bronze) ✅
- [x] Buat `source/services/kafka_consumer_svc.py` — subscribe per topic (bukan satu consumer untuk semua).
- [x] Buat `source/controller/consumer_to_gcs.py` — orchestrator & CLI command.
  - Gunakan `google-cloud-storage` library (via `StorageService`).
  - Gunakan ADC (lokal) atau `GCP_SERVICE_ACCOUNT_JSON` dari Infisical (Docker/prod).
  - Struktur path: `bronze/{entity_type}/year={}/month={}/day={}/batch_{}.json` (partisi Hive-style).
- [x] Implementasi logika batching: kumpulkan N pesan (atau tunggu max N detik) → jadikan 1 file `.json` → upload ke `bronze/`.
- [x] Test end-to-end: Pastikan file JSON muncul di GCS bucket `bronze` dengan struktur path yang benar.

---

## ✅ Fase 4: Snowflake Configuration (Integration)

Menghubungkan Snowflake ke GCS bucket `bronze` dan `silver`.

- [x] **Gunakan Terraform Snowflake** (`infrastructure/snowflake`):
   - Gunakan dual-provider (Snowflake dan Google) untuk otomatisasi penuh.
   - Buat Database `LAKEHOUSE_RAW`, Schema, dan Warehouse.
   - Buat `STORAGE INTEGRATION` di Snowflake.
- [x] **Auto Handshake di GCP:**
   - Terraform secara otomatis menarik identitas GCP Service Account bawaan Snowflake dan memberinya akses IAM `roles/storage.objectViewer` ke GCS bucket.
- [x] **Create External Stage** di Snowflake:
   - Terraform membuat stage eksternal yang terhubung ke bucket GCS menggunakan integration tersebut.
- [x] **Verifikasi:** Jalankan `LIST @LAKEHOUSE_RAW.BRONZE.GCS_BRONZE_STAGE;` di Snowflake Worksheet — file NDJSON harus terlihat.

---

## ✅ Fase 5: Databricks Configuration (Processing)

Databricks digunakan untuk mentransformasi raw NDJSON menjadi format Delta Lake.

- [x] **Cluster Setup:** Gunakan Databricks Serverless atau cluster biasa.
- [x] **Setup GCS Access:**
   - Gunakan Terraform Databricks untuk mem-provision Secret Scope (`gcp_secrets`).
   - Inject GCP Service Account JSON key dari Terraform ke dalam Secret Scope.
- [x] **Konfigurasi Python & Libraries:**
   - Gunakan Python library `google-cloud-storage`, `pandas`, `pyarrow`, dan `deltalake`.
- [x] **Tulis Driver-only Job** (`processing/databricks/bronze_to_silver.py`):
   - Arsitektur ini dirancang khusus untuk mengatasi limitasi Databricks Serverless (di mana `spark.conf` dan *worker filesystem access* diblokir).
   - Baca file NDJSON dari GCS Bronze secara paralel via Python Threads.
   - Parsing dan flattening struktur `EventEnvelope` dengan `pandas`.
   - Lakukan deduplikasi (*keep latest* berdasarkan primary key).
   - Tulis ke `/tmp` driver node menggunakan library Rust `deltalake` (via `pyarrow`).
   - Upload file `.delta` dari `/tmp` ke GCS Silver bucket.
- [x] **Eksekusi:** Jalankan notebook, pantau report sukses secara *real-time* per entity.


---

## ✅ Fase 6: Transformation (dbt)

Satu SQL codebase — dijalankan di dua platform untuk membuktikan portabilitas.
> **Arsitektur:** Silver layer dihasilkan oleh **Spark/Databricks** (Fase 5). dbt membaca dari Silver sebagai *source* dan menghasilkan **Gold layer** di BigQuery & Snowflake.

### 6.1 — Project Setup ✅
- [x] `dbt init transformation/dbt_project`
- [x] Setup `profiles.yml` dengan dua target:
  - `bigquery` → dataset BigQuery `ecommerce_gold`
  - `snowflake` → database Snowflake
- [x] Buat `models/sources.yml` — definisikan semua 8 entity Olist dari Silver layer sebagai dbt source.
- [x] Buat `macros/generate_schema_name.sql` — macro untuk konsistensi nama schema di dua platform.
- [x] Buat `macros/col.sql` — macro portable untuk parsing columns dari BigQuery & Snowflake.
- [x] Buat `macros/create_external_tables.sql` — macro untuk inisialisasi otomatis Snowflake external tables.

### 6.2 — Staging Models (Silver → dbt input) ✅
Satu model per entity, berisi type casting & renaming kolom:
- [x] `models/staging/stg_orders.sql`
- [x] `models/staging/stg_order_items.sql`
- [x] `models/staging/stg_order_payments.sql`
- [x] `models/staging/stg_order_reviews.sql`
- [x] `models/staging/stg_customers.sql`
- [x] `models/staging/stg_products.sql`
- [x] `models/staging/stg_sellers.sql`
- [x] `models/staging/stg_geolocation.sql`

### 6.3 — Intermediate Models (Business Logic) ✅
Join antar entity & agregasi awal:
- [x] `models/intermediate/int_orders_enriched.sql` — join orders + customers + order_items + payments + reviews

### 6.4 — Marts / Gold Models (Final Tables) ✅
Tabel siap konsumsi oleh BI / analyst:
- [x] `models/marts/dim_customers.sql` — dimensi customer dengan lifecycle value
- [x] `models/marts/fct_orders.sql` — fact table satu baris per transaksi dengan delivery time metric

### 6.5 — Data Quality (dbt Tests) ✅
- [x] Buat `schema.yml` per layer dengan tests:
  - `not_null` pada semua primary key
  - `unique` pada semua primary key
  - `relationships` antar staging models (FK integrity)
  - `accepted_values` untuk kolom status (order_status, payment_type)
- [x] Jalankan `dbt test --target bigquery` dan `dbt test --target snowflake`.
- [x] Dokumentasikan model dengan `description:` di setiap `schema.yml`.

### 6.6 — Run di Dua Platform ✅
```bash
# BigQuery: hasilkan tabel di dataset ecommerce_gold
dbt run --target bigquery
dbt test --target bigquery

# Snowflake: hasilkan tabel di Snowflake database
dbt run --target snowflake
dbt test --target snowflake

# Generate lineage documentation
dbt docs generate
dbt docs serve
```
- [x] Screenshot **lineage graph** dari `dbt docs serve` untuk README.

---

## ✅ Fase 7: Benchmark & Documentation

Ini adalah **inti komparasi** — output dari fase ini yang membuat project ini bernilai tinggi di portofolio.

### 7.1 — Performance Benchmark ✅
Jalankan query yang **identik** di kedua platform, catat hasilnya:

| Query Scenario | BigQuery (Cold / Warm) | Snowflake (Cold / Warm) | Data Scanned / Compute Cost |
|---|---|---|---|
| `fct_orders` full scan | 0.512s / 0.295s | 0.556s / 0.278s | BQ: 10.00 MB / SF: X-Small Warehouse |
| `group_by_aggregation` (LTV) | 0.317s / 0.240s | 0.322s / 0.286s | BQ: 10.00 MB / SF: X-Small Warehouse |
| `multi_join_enrichment` (Staging joins) | 1.271s / 1.074s | 0.438s / 0.375s | BQ: 20.00 MB / SF: X-Small Warehouse |
| `window_function_rfm` (Window RFM) | 0.331s / 0.282s | 0.336s / 0.313s | BQ: 10.00 MB / SF: X-Small Warehouse |
| `subquery_semi_join` (Filter Subquery) | 1.385s / 0.976s | 0.393s / 0.341s | BQ: 40.00 MB / SF: X-Small Warehouse |
| `rolling_30d_average` (Rolling Revenue) | 0.312s / 0.263s | 0.307s / 0.314s | BQ: 10.00 MB / SF: X-Small Warehouse |
| `string_operations_parsing` (String Parsing) | 1.244s / 0.967s | 0.387s / 0.364s | BQ: 20.00 MB / SF: X-Small Warehouse |
| `pivot_distribution` (Pivot States) | 1.303s / 0.866s | 0.369s / 0.384s | BQ: 40.00 MB / SF: X-Small Warehouse |
| `star_schema_join` (Star Schema Join) | 1.389s / 1.336s | 0.410s / 0.392s | BQ: 50.00 MB / SF: X-Small Warehouse |
| `percentile_analytics` (Percentile Median) | 0.452s / 0.434s | 0.303s / 0.314s | BQ: 20.00 MB / SF: X-Small Warehouse |

### 7.2 — Cost Analysis ✅
- [x] Estimasi biaya per 1 juta baris untuk masing-masing platform.
- [x] Catat perbedaan model pricing: **BigQuery** (per bytes scanned) vs **Snowflake** (per compute credits).

### 7.3 — Feature Comparison Table ✅
Dokumentasikan perbedaan fitur native:

| Feature | BigQuery | Snowflake |
|---|---|---|
| Time Travel | 7 hari (default) | 90 hari (default) |
| Zero-copy Clone | ❌ | ✅ |
| Materialized Views | ✅ | ✅ |
| Serverless | ✅ (fully) | ✅ (compute tier) |
| Streaming Insert | ✅ Native | ✅ via Snowpipe |
| Pricing Model | Bytes scanned | Compute credits |
| GCS Integration | ✅ Native | Via Storage Integration |
| dbt Support | ✅ | ✅ |

### 7.4 — README Final ✅
- [x] Arsitektur diagram end-to-end (Mermaid) — Kafka → Bronze → Silver → Gold → BI.
- [x] Benchmark results table (dari 7.1 & 7.2).
- [x] Feature comparison table (dari 7.3).
- [x] Screenshot dbt lineage graph.
- [x] Keputusan desain: kapan pilih BigQuery vs Snowflake.
- [x] *Lessons learned* dari membangun dual-paradigm pipeline.

---

## 🔲 Fase 8: Data Lineage, Governance & Observability

> Tiga pilar data engineering modern yang membedakan **data engineer junior** dari **senior/staff level** di perusahaan international.

---

### 8.1 — Data Lineage (End-to-End) 🔲

Tujuan: bisa menjawab **"data ini berasal dari mana, melewati proses apa, dan menghasilkan apa?"** untuk setiap kolom di Gold layer.

#### Tools: OpenLineage + Marquez

| Tool | Fungsi | Tier |
|---|---|---|
| **OpenLineage** | Open standard untuk emit lineage events (bukan tool, tapi protokol) | Gratis |
| **Marquez** | Reference backend OpenLineage — simpan & visualisasikan lineage graph | Gratis / Docker |
| **dbt** | Emit OpenLineage events native via `openlineage-integration-common` | Gratis |
| **Spark** | Emit OpenLineage events via `openlineage-spark` JAR | Gratis |

#### Implementasi:

- [ ] **Jalankan Marquez** via Docker Compose sebagai lineage metadata server:
  ```yaml
  # tambahkan ke docker-compose.yaml
  marquez:
    image: marquezproject/marquez:latest
    ports:
      - "5000:5000"  # API
      - "5001:5001"  # UI
  ```
- [ ] **Spark → OpenLineage:** Tambahkan `openlineage-spark` JAR ke Databricks cluster, set `OPENLINEAGE_URL` ke Marquez.
  - Spark akan otomatis emit lineage: `bronze/orders.json` → `silver/orders.delta`
- [ ] **dbt → OpenLineage:** Install `openlineage-dbt` dan set env `OPENLINEAGE_URL`:
  ```bash
  pip install openlineage-dbt
  OPENLINEAGE_URL=http://marquez:5000 dbt run --target bigquery
  ```
  - dbt akan otomatis emit: `silver.stg_orders` → `intermediate.int_orders_enriched` → `gold.fct_orders`
- [ ] **Column-level Lineage:** Aktifkan dbt column lineage (dbt 1.6+) — bisa trace sampai level kolom individual.
- [ ] **Verifikasi di Marquez UI** (`localhost:5001`): Pastikan graph menampilkan jalur penuh:
  ```
  CSV File → Kafka Topic → GCS Bronze → Spark → GCS Silver → dbt → BigQuery Gold
  ```
- [ ] Screenshot lineage graph end-to-end untuk README dan portofolio.

---

### 8.2 — Data Governance 🔲

Tujuan: memastikan data yang beredar di pipeline **diketahui, terdefinisi, dan terlindungi**.

#### 8.2.1 — Data Contracts (Schema Enforcement)
Data contract adalah perjanjian formal antara producer dan consumer tentang struktur data.

- [ ] Implementasi contract di level **Kafka Producer** — `EventEnvelope` + `schema_version` sudah menjadi fondasi ini ✅
- [ ] Buat file `contracts/` per entity sebagai dokumentasi formal:
  ```yaml
  # contracts/order_contract.yaml
  entity: order
  version: "1.0.0"
  owner: data-engineering-team
  topic: ecommerce.olist.orders.v1
  fields:
    - name: order_id
      type: string
      nullable: false
      pii: false
    - name: customer_id
      type: string
      nullable: false
      pii: true   # ← PII flag
    - name: order_status
      type: string
      accepted_values: [delivered, shipped, canceled, invoiced, processing]
  ```
- [ ] Tambahkan **validasi contract** di producer: tolak message jika struktur tidak sesuai contract.

#### 8.2.2 — PII Tagging & Data Classification
- [ ] Tandai kolom PII di `schema.yml` dbt dengan `meta: {pii: true}`:
  ```yaml
  - name: customer_unique_id
    description: "Unique identifier per customer"
    meta:
      pii: true
      classification: confidential
  ```
- [ ] Buat dbt macro `mask_pii()` untuk masking kolom sensitif di non-prod environment.
- [ ] Dokumentasikan klasifikasi data: `public` / `internal` / `confidential` / `restricted`.

#### 8.2.3 — dbt sebagai Lightweight Data Catalog
- [ ] Isi `description:` di setiap model dan kolom di `schema.yml`.
- [ ] Tambahkan `meta:` tags: owner, SLA, refresh_frequency, domain.
- [ ] Publish `dbt docs` sebagai internal data catalog (deploy ke GitHub Pages atau GCS static site).

---

### 8.3 — Data Observability 🔲

Tujuan: mendeteksi **masalah kualitas data secara otomatis** — tanpa harus menunggu complaint dari analyst.

#### 8.3.1 — dbt Elementary (Anomaly Detection)
Elementary adalah dbt package gratis yang menambahkan observability layer di atas dbt.

- [ ] Install elementary:
  ```bash
  # tambahkan ke packages.yml
  packages:
    - package: elementary-data/elementary
      version: [">=0.14.0"]
  ```
- [ ] Konfigurasi `elementary` profile di `profiles.yml` (terhubung ke BigQuery).
- [ ] Jalankan setelah setiap `dbt run`:
  ```bash
  dbt run --target bigquery
  edr report  # generate observability report HTML
  ```
- [ ] Elementary akan otomatis monitor:
  - **Volume anomaly** — jumlah row tiba-tiba drop/spike
  - **Freshness** — tabel belum diupdate sesuai SLA
  - **Schema changes** — kolom tiba-tiba hilang atau tipe berubah
  - **dbt test failures** — history test results overtime

#### 8.3.2 — Pipeline Audit Log
Setiap pipeline run harus meninggalkan jejak audit yang bisa di-query.

- [ ] Buat tabel `ecommerce_gold.pipeline_audit_log` di BigQuery:
  ```sql
  CREATE TABLE pipeline_audit_log (
    run_id STRING,
    pipeline_stage STRING,    -- 'ingestion' | 'spark' | 'dbt'
    entity_type STRING,
    source_topic STRING,
    rows_processed INT64,
    rows_failed INT64,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status STRING,            -- 'success' | 'failed' | 'partial'
    error_message STRING
  );
  ```
- [ ] Producer Python menulis ke audit log setiap batch selesai.
- [ ] dbt menulis ke audit log via `on-run-end` hook.

#### 8.3.3 — Kafka Consumer Lag Monitoring via kafka-exporter + Prometheus

Kafka-UI cukup untuk debugging manual, tapi untuk observability yang proper dibutuhkan **time-series metrics** yang bisa di-alert dan di-graph.

- [ ] Tambahkan stack monitoring ke `docker-compose.yaml`:
  ```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    command: ["--kafka.server=kafka:9092"]
    ports:
      - "9308:9308"  # expose Prometheus metrics

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
  ```
- [ ] Buat `monitoring/prometheus.yml` — scrape kafka-exporter di port 9308.
- [ ] Set alert rule di Prometheus: `kafka_consumer_lag_sum > 1000` selama 5 menit.

---

### 8.4 — Grafana Observability Dashboard 🔲

> **Mengapa Grafana, bukan Looker Studio?**
> Grafana dirancang untuk **operational/pipeline monitoring** (time-series, lag, error rate, latency).
> Looker Studio dirancang untuk **business intelligence** (revenue, customer analytics).
> Untuk observability pipeline — Grafana adalah pilihan yang digunakan di Uber, Airbnb, Gojek, dan perusahaan data-driven international.

#### Setup Datasource:
- [ ] Install plugin **Grafana BigQuery Datasource** untuk membaca `pipeline_audit_log`.
- [ ] Tambahkan Prometheus sebagai datasource kedua (untuk Kafka metrics).

#### Panel yang Dibuat:
- [ ] **Kafka Consumer Lag** (Prometheus) — time-series graph lag per topic per consumer group.
- [ ] **Pipeline Throughput** (BigQuery) — rows_processed per run, per entity_type, per jam.
- [ ] **Pipeline Error Rate** (BigQuery) — persentase `status = 'failed'` dari audit log.
- [ ] **dbt Run History** (BigQuery) — durasi dbt run overtime, berhasil vs gagal.
- [ ] **Schema Drift Alert** (BigQuery) — query ke elementary output table untuk schema changes.
- [ ] **End-to-End Latency** (BigQuery) — waktu dari `ingested_at` (Kafka) hingga tabel Gold terupdate.

#### Provisioning (Infrastructure as Code untuk Grafana):
- [ ] Buat `monitoring/grafana/provisioning/dashboards/pipeline_observability.json` — dashboard as code.
- [ ] Buat `monitoring/grafana/provisioning/datasources/datasources.yaml` — datasource config.
- [ ] Pastikan Grafana bisa di-spin up dengan `docker compose up` tanpa konfigurasi manual.
- [ ] Export dashboard JSON dan commit ke repo — ini menunjukkan **GitOps mindset**.

#### Screenshot untuk Portofolio:
- [ ] Screenshot dashboard panel Kafka lag + pipeline throughput + error rate.
- [ ] Embed di README sebagai bukti end-to-end observability.

---

### 8.5 — Business BI Dashboard (Opsional) 🔲

> Ini terpisah dari observability — Grafana untuk pipeline health, ini untuk business metrics dari Gold layer.

- [ ] Buat **Looker Studio** dashboard terhubung ke BigQuery Gold tables:
  - `fct_revenue_by_category` — revenue trend per kategori produk
  - `fct_seller_performance` — top seller by revenue & rating
  - `fct_orders` — order volume & delivery time heatmap
- [ ] Embed link publik Looker Studio di README.

---

> **Catatan Arsitektur Akhir:**
> Pipeline ini setelah Fase 8 mencakup seluruh spectrum modern data engineering:
>
> ```
> Kafka (8 topics) → GCS Bronze → Spark/Databricks → GCS Silver
>      → dbt (BigQuery + Snowflake) → Gold Layer
>      → OpenLineage/Marquez (Lineage)
>      → Data Contracts + PII Tagging (Governance)
>      → Elementary + Grafana + Prometheus (Observability)
> ```
>
> Kombinasi ini **sangat jarang** ditemukan di portofolio individual dan langsung menempatkan kamu di level **Senior / Staff Data Engineer** di perusahaan international.

---

## 🔲 Fase 9: Multi-Cloud Extension (AWS & Azure)

> Fase ini membuktikan bahwa pipeline yang dibangun bukan hanya berjalan di GCP, tapi **cloud-portable by design**.
> Menambah cloud baru **tidak menyentuh** kode Kafka, EventEnvelope, dbt SQL, atau observability stack.

---

### 9.0 — Apa yang Tidak Berubah (Cloud-Agnostic Core) ✅

Ini adalah **kekuatan arsitektur** yang harus ditonjolkan di README dan saat interview:

| Komponen | Alasan Cloud-Agnostic |
|---|---|
| Kafka + 8 topics | Protokol universal, berjalan di mana saja |
| `EventEnvelope` + `DATASET_TOPIC_MAP` | Pure Python, zero cloud dependency |
| Semua dbt SQL models (`stg_*`, `int_*`, `fct_*`) | SQL standar, hanya target yang berubah |
| Data Contracts (`contracts/*.yaml`) | Pure YAML |
| OpenLineage / Marquez | Open protocol, cloud-agnostic |
| Grafana + Prometheus | Self-hosted, berjalan di Docker |
| `StorageService` (ABC) | Interface-driven — provider diswap via factory |

---

### 9.1 — AWS Stack 🔲

#### 9.1.1 — Infrastructure (Terraform)
Buat modul Terraform baru **tanpa menyentuh** `infrastructure/gcp/`:
```
infrastructure/
  gcp/          ← existing, tidak disentuh
  aws/          ← NEW
    providers.tf        (AWS provider + region)
    main.tf             (S3 buckets: bronze, silver, gold)
    redshift.tf         (Amazon Redshift Serverless)
    iam.tf              (IAM role untuk Databricks on AWS / EMR)
    variables.tf
    terraform.tfvars
```

- [ ] Buat S3 buckets: `bronze`, `silver`, `gold` dengan struktur path Hive-style yang sama.
- [ ] Buat Redshift Serverless namespace & workgroup `ecommerce_gold`.
- [ ] Buat IAM role untuk akses Databricks on AWS (atau EMR).

#### 9.1.2 — Storage Adapter
`S3StorageService` sudah tersedia di `storage_svc.py` ✅
- [ ] Tambahkan `boto3` ke `pyproject.toml`.
- [ ] Tambahkan env var `STORAGE_PROVIDER=s3` + `AWS_REGION`, `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` ke Infisical.
- [ ] Ganti inisialisasi di `ingestion_ctl.py`:
  ```python
  storage = create_storage_service(provider=settings.STORAGE_PROVIDER, ...)
  ```
  Tidak ada perubahan lain di pipeline.

#### 9.1.3 — dbt Target Baru
- [ ] Tambahkan target `redshift` ke `profiles.yml` (SQL-nya **tidak berubah sama sekali**):
  ```yaml
  redshift:
    type: redshift
    host: "{{ env_var('AWS_REDSHIFT_HOST') }}"
    database: ecommerce_gold
    schema: public
    port: 5439
  ```
- [ ] Install adapter: `pip install dbt-redshift`
- [ ] Jalankan: `dbt run --target redshift` — hasilnya identik dengan BigQuery & Snowflake.
- [ ] `dbt test --target redshift` — jalankan test suite yang sama.

#### 9.1.4 — Delta Lake on AWS (Opsional)
- [ ] Jalankan Databricks on AWS (atau AWS EMR + Delta Lake JAR).
- [ ] PySpark job Bronze → Silver **tidak berubah** — hanya endpoint storage berbeda (S3 vs GCS).

---

### 9.2 — Azure Stack 🔲

#### 9.2.1 — Infrastructure (Terraform)
```
infrastructure/
  azure/        ← NEW
    providers.tf        (AzureRM provider)
    main.tf             (ADLS Gen2 containers: bronze, silver, gold)
    synapse.tf          (Azure Synapse Analytics)
    iam.tf              (Managed Identity untuk Databricks on Azure)
    variables.tf
    terraform.tfvars
```

- [ ] Buat ADLS Gen2 storage account + containers `bronze`, `silver`, `gold`.
- [ ] Buat Azure Synapse workspace `ecommerce-gold`.

#### 9.2.2 — Storage Adapter
`ADLSStorageService` sudah tersedia di `storage_svc.py` ✅
- [ ] Tambahkan `azure-storage-file-datalake` ke `pyproject.toml`.
- [ ] Tambahkan env var `STORAGE_PROVIDER=adls` ke Infisical.

#### 9.2.3 — dbt Target Baru
- [ ] Install adapter: `pip install dbt-synapse` atau `dbt-azuredatabricks`
- [ ] Tambahkan target `synapse` ke `profiles.yml`.
- [ ] Jalankan: `dbt run --target synapse`.

---

### 9.3 — Proof of Portability: Run Matrix 🔲

Bagian terpenting dari Fase 9 — jalankan pipeline yang **sama** di tiga cloud, catat hasilnya:

| Test | GCP | AWS | Azure |
|---|---|---|---|
| `dbt run` (semua models) | ✅ | 🔲 | 🔲 |
| `dbt test` (semua tests) | ✅ | 🔲 | 🔲 |
| `fct_orders` row count match | ✅ | 🔲 | 🔲 |
| Storage upload (Bronze) | GCS ✅ | S3 🔲 | ADLS 🔲 |
| Lineage terdeteksi di Marquez | ✅ | 🔲 | 🔲 |

- [ ] Buat tabel ini di README sebagai **Portability Matrix** — ini adalah visual proof yang sangat kuat.

---

### 9.4 — Environment Config per Cloud 🔲

Semua credential dikelola melalui Infisical (sudah ada), tambahkan environment baru:

```
Infisical Environments:
  dev-gcp    ← existing
  dev-aws    ← NEW: STORAGE_PROVIDER=s3, AWS_* vars
  dev-azure  ← NEW: STORAGE_PROVIDER=adls, AZURE_* vars
```

- [ ] Tambahkan `STORAGE_PROVIDER` sebagai env var di `settings.py`.
- [ ] Update `ingestion_ctl.py` untuk inisialisasi storage via `create_storage_service(settings.STORAGE_PROVIDER, ...)`.

---

> **Pesan untuk Rekruter:**
> *"Adding a new cloud provider to this pipeline requires: (1) a new Terraform module, (2) a new dbt adapter target, and (3) a new StorageService subclass. The Kafka ingestion layer, all dbt SQL models, data contracts, lineage tracking, and observability stack remain completely unchanged."*
> — Inilah yang dimaksud **production-grade extensible architecture**.
