# Databricks notebook source
# This file is a Databricks notebook — import via: Workspace → Import → File
# Format: Python (.py) with # COMMAND ---------- as cell separator

# MAGIC %md
# MAGIC # Bronze → Silver: PySpark Transformation
# MAGIC
# MAGIC Reads NDJSON batches from **GCS Bronze**, applies cleaning, deduplication,
# MAGIC and writes a **Delta Lake** table to **GCS Silver**.
# MAGIC
# MAGIC | Layer  | Format     | Location                        |
# MAGIC |--------|------------|---------------------------------|
# MAGIC | Bronze | NDJSON     | `gs://<bucket>/<entity>/year=../` |
# MAGIC | Silver | Delta Lake | `gs://<bucket>/<entity>/`       |
# MAGIC
# MAGIC > **Serverless Note:** This notebook runs entirely on the **driver** using
# MAGIC > the Python `google-cloud-storage` and `deltalake` libraries.
# MAGIC > No Spark workers are involved in reading or writing — this sidesteps
# MAGIC > all Databricks Serverless filesystem restrictions (DBFS disabled,
# MAGIC > `/Workspace` read-only for workers, `sparkContext` unavailable).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Install Dependencies
# MAGIC
# MAGIC **Must run first** — `%pip install` restarts the Python interpreter.

# COMMAND ----------

# MAGIC %pip install google-cloud-storage google-auth deltalake pyarrow --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Load Credentials from Databricks Secrets

# COMMAND ----------

import json
import io
import os
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Read all secrets from Databricks Secret Scope (provisioned via Terraform) ─
gcp_sa_json    = dbutils.secrets.get(scope="gcp_secrets", key="gcp_sa_key")
gcp_project_id = dbutils.secrets.get(scope="gcp_secrets", key="gcp_project_id")
gcs_bronze     = dbutils.secrets.get(scope="gcp_secrets", key="gcs_bronze_bucket")
gcs_silver     = dbutils.secrets.get(scope="gcp_secrets", key="gcs_silver_bucket")

print(f"[OK] Credentials loaded.")
print(f"     Project : {gcp_project_id}")
print(f"     Bronze  : gs://{gcs_bronze}")
print(f"     Silver  : gs://{gcs_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Build GCS Client & Verify Connectivity

# COMMAND ----------

from google.oauth2 import service_account
from google.cloud import storage as gcs_storage

sa_info     = json.loads(gcp_sa_json)
credentials = service_account.Credentials.from_service_account_info(sa_info)
gcs_client  = gcs_storage.Client(project=gcp_project_id, credentials=credentials)

try:
    blobs = list(gcs_client.bucket(gcs_bronze).list_blobs(max_results=5))
    print(f"[OK] GCS connected. Sample Bronze files:")
    for blob in blobs:
        print(f"     gs://{gcs_bronze}/{blob.name}")
except Exception as e:
    print(f"[ERROR] {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Define Entity Configuration

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List

@dataclass
class EntityConfig:
    name:        str
    primary_key: str
    date_cols:   List[str] = field(default_factory=list)

ENTITIES: List[EntityConfig] = [
    EntityConfig("order",            "order_id",                    ["order_purchase_timestamp", "order_delivered_customer_date", "order_approved_at", "order_delivered_carrier_date", "order_estimated_delivery_date"]),
    EntityConfig("order_item",       "order_item_id",               []),
    EntityConfig("payment",          "order_id",                    []),
    EntityConfig("review",           "review_id",                   ["review_creation_date", "review_answer_timestamp"]),
    EntityConfig("customer",         "customer_id",                 []),
    EntityConfig("product",          "product_id",                  []),
    EntityConfig("product_category", "product_category_name",       []),
    EntityConfig("seller",           "seller_id",                   []),
    EntityConfig("geolocation",      "geolocation_zip_code_prefix", []),
]

print(f"[OK] {len(ENTITIES)} entities registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Define Transformation Functions
# MAGIC
# MAGIC **Architecture**: All processing runs on the driver using pandas + deltalake.
# MAGIC No Spark workers involved — 100% compatible with Databricks Serverless.

# COMMAND ----------

import pandas as pd
from deltalake.writer import write_deltalake

# ── I/O Helpers ───────────────────────────────────────────────────────────────

def _download_blob(blob) -> List[str]:
    """Download one GCS blob, return its non-empty NDJSON lines."""
    content = blob.download_as_text(encoding="utf-8")
    return [l.strip() for l in content.splitlines() if l.strip()]


def read_entity_as_pandas(entity_name: str) -> pd.DataFrame:
    """
    Download all NDJSON files for one entity from GCS Bronze.
    Returns a flat pandas DataFrame with payload + metadata fields.
    Runs entirely on the driver — no Spark, no DBFS, no /Workspace writes.
    """
    blobs = list(gcs_client.bucket(gcs_bronze).list_blobs(prefix=f"{entity_name}/"))
    if not blobs:
        raise FileNotFoundError(f"No files at gs://{gcs_bronze}/{entity_name}/")

    print(f"  [READ]  {len(blobs)} files found. Downloading in parallel...")

    all_lines: List[str] = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for lines in executor.map(_download_blob, blobs):
            all_lines.extend(lines)

    print(f"  [READ]  {len(all_lines):,} NDJSON lines downloaded.")

    # Parse NDJSON → pandas
    raw_df = pd.read_json(io.StringIO("\n".join(all_lines)), lines=True)

    # Unpack EventEnvelope: {metadata: {...}, payload: {...}}
    if "payload" in raw_df.columns and "metadata" in raw_df.columns:
        payload_df  = pd.json_normalize(raw_df["payload"])
        metadata_df = pd.json_normalize(raw_df["metadata"])
        return pd.concat([payload_df, metadata_df], axis=1)
    elif "payload" in raw_df.columns:
        return pd.json_normalize(raw_df["payload"])
    else:
        return raw_df


def upload_local_dir_to_gcs(local_dir: str, gcs_bucket_name: str, gcs_prefix: str) -> int:
    """
    Upload all files under a local directory to GCS using parallel threads.
    Python's open() on /tmp works fine on the Databricks driver node.
    """
    tasks = []
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_file = os.path.join(root, filename)
            rel_path   = os.path.relpath(local_file, local_dir).replace("\\", "/")
            blob_name  = f"{gcs_prefix}/{rel_path}"
            tasks.append((local_file, blob_name))

    def _upload(args):
        local_path, blob_name = args
        with open(local_path, "rb") as fh:
            gcs_client.bucket(gcs_bucket_name).blob(blob_name).upload_from_file(fh)
        return 1

    print(f"  [UPLOAD] Uploading {len(tasks)} Delta files in parallel...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        uploaded = sum(executor.map(_upload, tasks))

    print(f"  [UPLOAD] {uploaded} files → gs://{gcs_bucket_name}/{gcs_prefix}/")
    return uploaded


# ── Core Transformation ───────────────────────────────────────────────────────

def process_entity(cfg: EntityConfig) -> dict:
    """
    Bronze → Silver pipeline for one entity (fully driver-side, no Spark workers):

    1. Read   — Download NDJSON from GCS Bronze in parallel
    2. Unpack — Flatten EventEnvelope {metadata, payload} → flat DataFrame
    3. Cast   — Convert date strings → datetime (errors='coerce' → NaT)
    4. Dedup  — Keep latest record per primary_key
    5. Write  — Save as Delta Lake to /tmp (driver local fs)
    6. Upload — Transfer Delta files from /tmp → GCS Silver
    """
    local_delta = f"/tmp/silver/{cfg.name}"
    SEPARATOR   = "=" * 60

    print(f"\n{SEPARATOR}")
    print(f"Entity  : {cfg.name.upper()}")
    print(f"Bronze  : gs://{gcs_bronze}/{cfg.name}/")
    print(f"Silver  : gs://{gcs_silver}/{cfg.name}/")

    # 1 + 2 — Read & Unpack
    df        = read_entity_as_pandas(cfg.name)
    raw_count = len(df)
    assert raw_count > 0, f"[DQ FAIL] No records in Bronze for '{cfg.name}'"
    print(f"  [SHAPE] {raw_count:,} rows × {len(df.columns)} columns")

    # 3 — Type casting (errors='coerce': invalid/empty → NaT, never crashes)
    for col_name in cfg.date_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce", utc=True)

    df["_silver_loaded_at"] = pd.Timestamp.now(tz="UTC")

    # 4 — Deduplicate: keep latest row per primary key
    pk = cfg.primary_key
    if pk in df.columns:
        order_col = "ingested_at" if "ingested_at" in df.columns else "_silver_loaded_at"
        df = (df
              .sort_values(order_col, ascending=False)
              .drop_duplicates(subset=[pk])
              .reset_index(drop=True))

    silver_count = len(df)
    duplicates   = raw_count - silver_count
    print(f"  [DEDUP] {raw_count:,} raw → {silver_count:,} unique ({duplicates:,} duplicates dropped)")

    # 5 — Write Delta Lake to driver /tmp (no Spark workers, no filesystem restrictions)
    if os.path.exists(local_delta):
        shutil.rmtree(local_delta)

    # Convert to PyArrow Table explicitly — required by deltalake >= 0.10
    # Sanitize object columns: replace Python None/nan with proper nulls
    import pyarrow as pa
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].where(df[col].notna(), other=None)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(local_delta, arrow_table, mode="overwrite")

    delta_files = sum(len(files) for _, _, files in os.walk(local_delta))
    print(f"  [DELTA] Written to /tmp: {local_delta} ({delta_files} files)")

    # 6 — Upload /tmp Delta → GCS Silver
    uploaded = upload_local_dir_to_gcs(local_delta, gcs_silver, cfg.name)

    return {
        "entity":             cfg.name,
        "raw_count":          raw_count,
        "silver_count":       silver_count,
        "duplicates_dropped": duplicates,
        "silver_path":        f"gs://{gcs_silver}/{cfg.name}/",
        "files_uploaded":     uploaded,
        "status":             "SUCCESS",
    }


print("[OK] All functions defined. Ready to run Cell 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Run Pipeline — One Entity at a Time
# MAGIC
# MAGIC Each entity is processed and **immediately reported** before moving to the next.
# MAGIC If one entity fails, the pipeline continues with the rest.

# COMMAND ----------

import time

results = []
errors  = []

for cfg in ENTITIES:
    start = time.time()
    try:
        result = process_entity(cfg)
        elapsed = time.time() - start
        result["elapsed_sec"] = round(elapsed, 1)
        results.append(result)

        # ── Immediate success report ──────────────────────────────────────────
        print(f"  ✅ SUCCESS  | {cfg.name:<20} | "
              f"{result['silver_count']:>6,} rows | "
              f"{result['files_uploaded']:>3} files | "
              f"{elapsed:.1f}s")

    except AssertionError as dq_err:
        elapsed = time.time() - start
        print(f"  ⚠️  DQ FAIL  | {cfg.name:<20} | {dq_err}")
        errors.append({"entity": cfg.name, "error": str(dq_err), "status": "DQ_FAIL", "elapsed_sec": round(elapsed, 1)})
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ❌ ERROR    | {cfg.name:<20} | {e}")
        errors.append({"entity": cfg.name, "error": str(e), "status": "ERROR", "elapsed_sec": round(elapsed, 1)})

print("\n" + "=" * 60)
print(f"DONE  ✅ {len(results)} succeeded  ❌ {len(errors)} failed  (total: {len(ENTITIES)} entities)")
print("=" * 60)

if results:
    display(spark.createDataFrame(results))

if not errors:
    print("\n✅ All entities processed successfully!")
else:
    for e in errors:
        print(f"  ✗ {e['entity']}: {e['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Verify Silver Output (Spot Check)
# MAGIC
# MAGIC Reads the Delta table back from driver `/tmp` using the `deltalake` Python library.

# COMMAND ----------

from deltalake import DeltaTable

VERIFY_ENTITY  = "order"
local_verify   = f"/tmp/silver/{VERIFY_ENTITY}"

print(f"Verifying Delta table: '{VERIFY_ENTITY}' at {local_verify}\n")

dt      = DeltaTable(local_verify)
verify_df = dt.to_pandas()

print(f"Schema  : {list(verify_df.dtypes.items())}")
print(f"Rows    : {len(verify_df):,}")
print(f"Columns : {len(verify_df.columns)}")

# Data quality check
null_pk = verify_df["order_id"].isna().sum()
assert null_pk == 0, f"[DQ FAIL] {null_pk} rows with NULL order_id in Silver!"
print(f"\n[DQ PASS] Zero NULL primary keys — Silver data is clean ✅")

display(spark.createDataFrame(verify_df.head(5)))
