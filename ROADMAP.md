## **🏗️ Fase 1: Identity & Security (The Foundation)**

Sebelum menyentuh *tools* apapun, kita amankan pintunya.

1. **GCP Project:** Buat project di GCP Console.  
2. **Service Account (SA):** Buat SA cloud-migrator dengan role Editor.  
3. **IAM Binding:** Berikan email Gmail pribadimu role Service Account Token Creator pada SA tersebut.  
4. **Local Auth:** Jalankan gcloud auth application-default login di terminal laptop.

## ---

**🛠️ Fase 2: Infrastructure as Code (Terraform)**

Otomatisasi pembuatan "wadah" data.

1. **Providers:** Setup providers.tf dengan metode **Impersonation**.  
2. **Resources:** Buat file main.tf untuk men-deploy:  
   * **GCS Buckets:** bronze, silver, dan gold.  
   * **BigQuery Dataset:** Sebagai target Lakehouse versi Google.  
3. **Command:** terraform init \-\> terraform apply.

## ---

**🐍 Fase 3: Ingestion Engine (Python & Kafka)**

Memindahkan data dari sumber ke Cloud.

1. **Engine Setup:** Masukkan *Clean Architecture* Python kamu ke folder ingestion/.  
2. **Kafka:** Jalankan Kafka di Minikube. Buat producer untuk data eCommerce.  
3. **Consumer to GCS:** Gunakan library google-cloud-storage di Python. Karena sudah ada ADC, script kamu akan langsung bisa *upload* ke bucket bronze tanpa file JSON.

## ---

**❄️ Fase 4: Snowflake Configuration (Integration)**

Ini adalah langkah yang tadi terlewat. Kita hubungkan Snowflake ke GCS tanpa kunci.

1. **Create Integration di Snowflake:**  
   Jalankan perintah ini di *Snowflake Worksheet*:  
   SQL  
   CREATE STORAGE INTEGRATION gcs\_int  
     TYPE \= EXTERNAL\_STAGE  
     STORAGE\_ALLOWED\_LOCATIONS \= ('gcs://bronze-bucket-name/')  
     ENABLED \= TRUE;

2. **Ambil Identitas Snowflake:**  
   Jalankan DESC STORAGE INTEGRATION gcs\_int;. Ambil nilai pada kolom **STORAGE\_GCP\_SERVICE\_ACCOUNT** (biasanya berupa email).  
3. **Handshake di GCP:**  
   Buka GCP Console \> GCS Bucket bronze \> Permissions. Tambahkan email dari Snowflake tadi sebagai **Principal** dengan role **Storage Object Viewer**.  
4. **Create Stage:** Di Snowflake, buat *Stage* yang mengarah ke bucket GCS tersebut. Sekarang Snowflake bisa membaca data di GCS secara *real-time*.

## ---

**🧱 Fase 5: Databricks Configuration (Processing)**

Databricks akan berperan sebagai mesin pengolah (Spark) di atas GCS.

1. **Cluster Setup:** Buat cluster di Databricks (bisa gunakan Community Edition).  
2. **GCS Access:** Ada dua cara *keyless* di Databricks:  
   * **Metode 1 (Mounting):** Menggunakan *Instance Profile* jika Databricks berjalan di dalam GCP.  
   * **Metode 2 (Session Token):** Menggunakan token sementara dari Service Account yang di-generate via Python/CLI.  
3. **Spark Job:** Tulis kode PySpark untuk:  
   * Membaca JSON dari bronze.  
   * Melakukan transformasi (cleansing/filtering).  
   * Menyimpan ke folder silver dalam format **Delta Lake**.

## ---

**🔄 Fase 6: Transformation (dbt)**

Mengelola logika bisnis di satu tempat untuk dua platform berbeda.

1. **Project Setup:** Inisialisasi dbt di folder transformation/.  
2. **Profiles.yml:** Konfigurasi dua target: bigquery dan snowflake.  
3. **Unified Models:** Buat model SQL (misal: dim\_products.sql). Dengan dbt, kode yang sama bisa dijalankan di BigQuery maupun Snowflake untuk membandingkan hasilnya.

## ---

**📊 Fase 7: Analysis & Documentation**

Langkah terakhir untuk mempercantik portofoliomu.

1. **Benchmark:** Bandingkan performa *query* dan biaya antara BigQuery dan Snowflake.  
2. **Lineage:** Generate dokumentasi dbt (dbt docs generate) untuk melihat alur data.  
3. **README:** Tuliskan temuanmu di README.md utama, sertakan diagram arsitektur yang sudah kita bahas.

