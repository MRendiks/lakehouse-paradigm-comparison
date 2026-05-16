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

## 🔄 Fase 3: Ingestion Engine (Python & Kafka) *(IN PROGRESS)*

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
- [ ] Jalankan Kafka & Kafka-UI via **Docker Compose** (sudah ada di `docker-compose.yaml`).
- [ ] Desain skema data eCommerce (misal: `Order`, `Product`, `User`) dalam format JSON.
- [ ] Buat `source/services/kafka_producer.py` — service untuk mempublikasikan pesan ke topic Kafka.
- [ ] Buat `source/controller/ingestion_ctl.py` — command CLI `engine` yang memanggil producer.
- [ ] Test end-to-end: Pastikan pesan terkirim dan terlihat di **Kafka-UI** (`localhost:8080`).

### 3.3 — Kafka Consumer → GCS (Bronze) 🔲
- [ ] Buat `source/services/kafka_consumer.py` — service untuk men-subscribe topic Kafka.
- [ ] Buat `source/services/gcs_uploader.py` — service untuk upload file JSON ke GCS bucket `bronze`.
  - Gunakan `google-cloud-storage` library.
  - Gunakan ADC (lokal) atau `GCP_SERVICE_ACCOUNT_JSON` dari Infisical (Docker/prod).
- [ ] Implementasi logika batching: kumpulkan N pesan → jadikan 1 file `.json` → upload ke `bronze/`.
- [ ] Test end-to-end: Pastikan file JSON muncul di GCS bucket `bronze`.

---

## 🔲 Fase 4: Snowflake Configuration (Integration)

Menghubungkan Snowflake ke GCS bucket `bronze` tanpa file kunci/JSON.

1. **Create Storage Integration** di Snowflake Worksheet:
   ```sql
   CREATE STORAGE INTEGRATION gcs_int
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'GCS'
     STORAGE_ALLOWED_LOCATIONS = ('gcs://nama-bronze-bucket/')
     ENABLED = TRUE;
   ```
2. **Ambil Identitas Snowflake:** Jalankan `DESC INTEGRATION gcs_int;` dan catat nilai `STORAGE_GCP_SERVICE_ACCOUNT` (sebuah email GCP milik Snowflake).
3. **Handshake di GCP:**
   - Buka GCP Console → GCS Bucket `bronze` → tab **Permissions**.
   - Tambahkan email Snowflake sebagai **Principal** dengan role **`Storage Object Viewer`**.
4. **Create External Stage** di Snowflake:
   ```sql
   CREATE STAGE my_gcs_stage
     URL = 'gcs://nama-bronze-bucket/'
     STORAGE_INTEGRATION = gcs_int;
   ```
5. **Verifikasi:** Jalankan `LIST @my_gcs_stage;` — file JSON dari Fase 3 harus terlihat.

---

## 🔲 Fase 5: Databricks Configuration (Processing)

Databricks sebagai mesin pengolah Spark di atas GCS.

1. **Cluster Setup:** Buat cluster di Databricks (gunakan *Community Edition* atau akun trial).
2. **Setup GCS Access (Keyless):**
   - Buat *Service Account* GCP baru untuk Databricks dengan role `Storage Object Admin` pada bucket `bronze` & `silver`.
   - Copy isi file JSON SA → simpan sebagai *Databricks Secret* (`dbutils.secrets`).
3. **Konfigurasi Spark di Notebook:**
   ```python
   spark.conf.set("fs.gs.auth.service.account.json.keyfile", dbutils.secrets.get(...))
   ```
4. **Tulis PySpark Job** (`transformation/spark/bronze_to_silver.py`):
   - Baca JSON dari `bronze/` → parse & validasi skema.
   - Lakukan transformasi (cleansing, filtering, type casting).
   - Simpan ke `silver/` dalam format **Delta Lake** (`.delta`).
5. **Test:** Verifikasi file Delta muncul di GCS bucket `silver`.

---

## 🔲 Fase 6: Transformation (dbt)

Mengelola logika bisnis SQL di satu tempat untuk dua platform.

1. **Inisialisasi dbt Project:**
   ```bash
   dbt init transformation
   ```
2. **Setup `profiles.yml`** dengan dua *target*:
   - `bigquery` — terhubung ke dataset BigQuery `ecommerce_gold`.
   - `snowflake` — terhubung ke database Snowflake.
3. **Buat Source Definition** (`models/sources.yml`) yang menunjuk ke tabel/stage dari Fase 3-5.
4. **Buat Unified SQL Models:**
   - `models/staging/stg_orders.sql`
   - `models/staging/stg_products.sql`
   - `models/marts/dim_products.sql`
   - `models/marts/fct_orders.sql`
5. **Jalankan di dua platform:**
   ```bash
   dbt run --target bigquery
   dbt run --target snowflake
   ```
6. **Verifikasi:** Bandingkan hasil query dan output dari kedua platform.

---

## 🔲 Fase 7: Analysis & Documentation

Langkah akhir untuk mempercantik portofolio.

1. **Benchmark Performa & Biaya:**
   - Jalankan query yang sama di BigQuery dan Snowflake.
   - Catat: waktu eksekusi, bytes di-scan, dan estimasi biaya.
2. **Generate Lineage Diagram:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```
3. **Tulis README.md Utama:**
   - Arsitektur diagram (Mermaid atau gambar).
   - Perbandingan hasil benchmark BigQuery vs Snowflake.
   - Keputusan desain dan *lessons learned*.
