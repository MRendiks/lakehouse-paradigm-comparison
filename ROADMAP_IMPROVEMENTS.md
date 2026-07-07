# 🗺️ Roadmap: 5 Area Perbaikan Lakehouse Project

> Disusun berdasarkan evaluasi Senior Data Engineer terhadap `lakehouse-paradigm-comparison`.
> Setiap improvement dilengkapi langkah-langkah detail yang bisa langsung dieksekusi.

---

## Daftar Isi

1. [Fix CI/CD Pipeline (Mismatch Tools)](#1-fix-cicd-pipeline)
2. [Tambahkan Real Test Coverage](#2-tambahkan-real-test-coverage)
3. [Enforce Data Contracts dengan Validasi Runtime](#3-enforce-data-contracts)
4. [Implement Incremental Models untuk dbt](#4-implement-incremental-models)
5. [Hapus Secrets & Hardcoded IDs](#5-hapus-secrets--hardcoded-ids)

---

## 1. Fix CI/CD Pipeline

**Problem**: CI menggunakan `flake8` + `requirements.txt`, tapi project pakai `ruff` + `uv` + `pyproject.toml`.

### 1.1 — Update Python CI

File: `.github/workflows/python-app-ci.yml`

```yaml
name: Python CI

on:
  push:
    paths:
      - 'ingestion/**'
      - 'transformation/**'
  pull_request:
    paths:
      - 'ingestion/**'
      - 'transformation/**'

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        path: ['ingestion', 'transformation/dbt_project']

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Set up Python
        run: uv python install 3.10

      - name: Install dependencies
        working-directory: ${{ matrix.path }}
        run: uv sync

      - name: Lint with ruff
        if: matrix.path == 'ingestion'
        working-directory: ${{ matrix.path }}
        run: uv run ruff check source/

      - name: Format check with ruff
        if: matrix.path == 'ingestion'
        working-directory: ${{ matrix.path }}
        run: uv run ruff format --check source/

      - name: Run tests
        working-directory: ${{ matrix.path }}
        run: uv run pytest tests/ -v --tb=short
```

### 1.2 — Update Terraform CI

File: `.github/workflows/terraform-ci.yml`

```yaml
name: Terraform CI

on:
  pull_request:
    paths:
      - 'infrastructure/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        dir: ['infrastructure/gcp', 'infrastructure/databricks', 'infrastructure/snowflake']

    steps:
      - uses: actions/checkout@v4

      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.8"

      - name: Terraform fmt
        working-directory: ${{ matrix.dir }}
        run: terraform fmt -check -recursive

      - name: Terraform init
        working-directory: ${{ matrix.dir }}
        run: terraform init -backend=false

      - name: Terraform validate
        working-directory: ${{ matrix.dir }}
        run: terraform validate
```

### 1.3 — Verifikasi Lokal

```bash
cd ingestion
uv run ruff check source/
uv run ruff format --check source/
uv run pytest tests/ -v
```

---

## 2. Tambahkan Real Test Coverage

**Problem**: Test hanya placeholder `self.assertTrue(True)`.

### 2.1 — Unit Test untuk EventEnvelope

File baru: `ingestion/tests/unit/test_event_envelope.py`

```python
import pytest
from source.models.event_envelope import EventEnvelope, EventMetadata
from source.core.constants import OlistTopic


class TestEventEnvelope:
    """Test factory method and serialization of EventEnvelope."""

    def test_create_produces_valid_envelope(self):
        """EventEnvelope.create() should populate all metadata fields."""
        payload = {"order_id": "abc123", "status": "delivered"}

        envelope = EventEnvelope.create(
            topic=OlistTopic.ORDERS,
            entity_type="order",
            payload=payload,
            pipeline_run_id="run-test-001",
            ingestion_env="dev",
        )

        assert envelope.metadata.entity_type == "order"
        assert envelope.metadata.source_topic == OlistTopic.ORDERS.value
        assert envelope.metadata.event_type == "olist.order.created"
        assert envelope.metadata.pipeline_run_id == "run-test-001"
        assert envelope.metadata.ingestion_env == "dev"
        assert envelope.metadata.schema_version == "1.0.0"
        assert envelope.metadata.checksum is not None
        assert envelope.payload == payload

    def test_checksum_is_deterministic(self):
        """Same payload should produce same checksum."""
        payload = {"a": 1, "b": 2}

        e1 = EventEnvelope.create(OlistTopic.ORDERS, "order", payload)
        e2 = EventEnvelope.create(OlistTopic.ORDERS, "order", payload)

        assert e1.metadata.checksum == e2.metadata.checksum

    def test_checksum_changes_with_payload(self):
        """Different payload should produce different checksum."""
        e1 = EventEnvelope.create(OlistTopic.ORDERS, "order", {"x": 1})
        e2 = EventEnvelope.create(OlistTopic.ORDERS, "order", {"x": 2})

        assert e1.metadata.checksum != e2.metadata.checksum

    def test_kafka_key_uses_entity_id(self):
        """kafka_key() should extract {entity}_id from payload."""
        envelope = EventEnvelope.create(
            topic=OlistTopic.ORDERS,
            entity_type="order",
            payload={"order_id": "ORD-999"},
        )

        assert envelope.kafka_key() == "ORD-999"

    def test_kafka_key_falls_back_to_event_id(self):
        """When no matching *_id field, fallback to event_id."""
        envelope = EventEnvelope.create(
            topic=OlistTopic.ORDERS,
            entity_type="order",
            payload={"some_field": "value"},
        )

        assert envelope.kafka_key() == envelope.metadata.event_id

    def test_to_kafka_message_serializable(self):
        """to_kafka_message() should return a JSON-safe dict."""
        import json

        envelope = EventEnvelope.create(
            topic=OlistTopic.PRODUCTS,
            entity_type="product",
            payload={"product_id": "P1", "name": "Laptop"},
        )

        msg = envelope.to_kafka_message()

        # Should not raise
        json.dumps(msg)
        assert "metadata" in msg
        assert "payload" in msg
```

### 2.2 — Unit Test untuk TopicRouter

File baru: `ingestion/tests/unit/test_topic_router.py`

```python
import pytest
from source.mapper.handler.topic_router import TopicRouter
from source.core.constants import OlistTopic


class TestTopicRouter:
    """Test CSV filename → Kafka topic resolution."""

    def test_resolve_known_filename(self):
        router = TopicRouter()
        result = router.resolve("olist_orders_dataset.csv")

        assert result is not None
        assert result["topic"] == OlistTopic.ORDERS
        assert result["entity_type"] == "order"

    def test_resolve_all_mapped_files(self):
        """Every registered filename should resolve."""
        router = TopicRouter()
        mappings = {
            "olist_orders_dataset.csv": "order",
            "olist_order_items_dataset.csv": "order_item",
            "olist_order_payments_dataset.csv": "payment",
            "olist_order_reviews_dataset.csv": "review",
            "olist_customers_dataset.csv": "customer",
            "olist_products_dataset.csv": "product",
            "olist_product_category_name_translation.csv": "product_category",
            "olist_sellers_dataset.csv": "seller",
            "olist_geolocation_dataset.csv": "geolocation",
        }

        for filename, expected_entity in mappings.items():
            result = router.resolve(filename)
            assert result is not None, f"Failed to resolve: {filename}"
            assert result["entity_type"] == expected_entity

    def test_unknown_filename_returns_none(self):
        router = TopicRouter()
        result = router.resolve("unknown_file.csv")
        assert result is None

    def test_resolve_or_raise_for_known_file(self):
        router = TopicRouter()
        result = router.resolve_or_raise("olist_orders_dataset.csv")
        assert result["entity_type"] == "order"

    def test_resolve_or_raise_throws_for_unknown(self):
        router = TopicRouter()
        with pytest.raises(ValueError):
            router.resolve_or_raise("nonexistent.csv")
```

### 2.3 — Unit Test untuk CsvReader

File baru: `ingestion/tests/unit/test_csv_reader.py`

```python
import tempfile
from pathlib import Path
import pytest
from source.mapper.handler.csv_reader import CsvReader


class TestCsvReader:
    """Test CSV reading and normalization."""

    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV file for testing."""
        content = (
            "order_id,customer_id,order_status\n"
            "ORD-001,CUST-A,delivered\n"
            "ORD-002,CUST-B,shipped\n"
            "ORD-003,CUST-C,processing\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = f.name

        yield Path(path)
        # Cleanup
        Path(path).unlink(missing_ok=True)

    def test_iter_rows_yields_all_rows(self, sample_csv):
        reader = CsvReader(sample_csv)
        rows = list(reader.iter_rows())

        assert len(rows) == 3
        # First element is row_number, second is payload dict
        row_num, payload = rows[0]
        assert row_num == 1
        assert payload["order_id"] == "ORD-001"

    def test_filename_property(self, sample_csv):
        reader = CsvReader(sample_csv)
        assert reader.filename == sample_csv.name

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            CsvReader(Path("/nonexistent/file.csv"))
```

### 2.4 — Integration Test Skeleton

Update: `ingestion/tests/integration/test_integration.py`

```python
import os
import pytest


@pytest.mark.integration
class TestIntegration:
    """Integration tests — require running Kafka and GCS emulators."""

    @pytest.mark.skipif(
        not os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        reason="KAFKA_BOOTSTRAP_SERVERS not set — set to run integration tests",
    )
    def test_kafka_connection(self):
        """Verify Kafka broker is reachable and can produce/consume."""
        from confluent_kafka import Producer, Consumer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        test_topic = "test.integration.v1"

        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(test_topic, b'{"test": "hello"}')
        producer.flush(timeout=5)

        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "test-integration-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([test_topic])

        msg = consumer.poll(timeout=5)
        assert msg is not None
        assert not msg.error()

        consumer.close()

    @pytest.mark.skipif(
        not os.getenv("GCS_BRONZE_BUCKET"),
        reason="GCS_BRONZE_BUCKET not set",
    )
    def test_gcs_upload_download(self):
        """Verify GCS upload and object existence."""
        from source.services.storage_svc import GCSStorageService

        project_id = os.getenv("GCP_PROJECT_ID", "test")
        bucket = os.getenv("GCS_BRONZE_BUCKET", "test-bucket")

        svc = GCSStorageService(project_id=project_id)
        test_path = "test/integration_test.json"

        uri = svc.upload_bytes(
            bucket=bucket,
            path=test_path,
            data=b'{"test": true}',
        )
        assert uri.startswith("gs://")
```

### 2.5 — Jalankan Tests

```bash
cd ingestion

# Unit tests only (no integration)
uv run pytest tests/unit/ -v

# All tests (skip integration if no env vars)
uv run pytest tests/ -v --tb=short
```

---

## 3. Enforce Data Contracts

**Problem**: `order_contract.yaml` ada tapi tidak pernah digunakan untuk validasi runtime.

### 3.1 — Buat Contract Validator

File baru: `ingestion/source/core/contract_validator.py`

```python
"""
contract_validator.py — Validasi payload terhadap data contract (YAML).

Digunakan oleh producer sebelum mengirim message ke Kafka.
Route: Data Contract → ValidationError → DLQ (tidak masuk Kafka).
"""

from pathlib import Path
from typing import Any

import yaml
from loguru import logger


class ContractValidationError(Exception):
    """Exception untuk data yang melanggar kontrak."""

    def __init__(self, field: str, expected: str, actual: Any, row_num: int):
        self.field = field
        self.expected = expected
        self.actual = actual
        self.row_num = row_num
        super().__init__(
            f"[Row #{row_num}] Field '{field}': expected {expected}, got {type(actual).__name__} = {actual}"
        )


class ContractValidator:
    """
    Memuat kontrak YAML dan memvalidasi payload row-by-row.

    Usage:
        validator = ContractValidator("contracts/order_contract.yaml")
        validator.validate(payload, row_num=42)
        # Raises ContractValidationError jika gagal
    """

    # Mapping: YAML type string → Python type
    TYPE_MAP = {
        "string": str,
        "integer": int,
        "float": float,
        "numeric": (int, float),
        "timestamp": str,  # ISO 8601 string
        "boolean": bool,
    }

    def __init__(self, contract_path: str | Path) -> None:
        self._contract_path = Path(contract_path)
        self._contract = self._load()
        self._field_rules = self._parse_fields()
        logger.info(
            f"ContractValidator loaded | entity={self._contract['entity']} "
            f"v{self._contract['version']} | {len(self._field_rules)} fields"
        )

    def _load(self) -> dict:
        if not self._contract_path.exists():
            raise FileNotFoundError(f"Contract file not found: {self._contract_path}")
        with open(self._contract_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _parse_fields(self) -> dict:
        """Build dict: {field_name: {type, nullable, accepted_values}}"""
        rules = {}
        for field in self._contract.get("fields", []):
            rules[field["name"]] = {
                "type": field.get("type", "string"),
                "nullable": field.get("nullable", True),
                "accepted_values": field.get("accepted_values", None),
            }
        return rules

    def validate(self, payload: dict, row_num: int = 0) -> None:
        """
        Validasi payload terhadap kontrak.

        Raises ContractValidationError pada kegagalan pertama.
        Tidak memvalidasi field yang tidak ada di kontrak (forward-compatible).
        """
        for field_name, rules in self._field_rules.items():
            value = payload.get(field_name)

            # ── Nullability Check ──
            if value is None or value == "":
                if not rules["nullable"]:
                    raise ContractValidationError(
                        field=field_name,
                        expected="not null",
                        actual="null",
                        row_num=row_num,
                    )
                continue

            # ── Type Check ──
            expected_type = rules["type"]
            python_type = self.TYPE_MAP.get(expected_type)
            if python_type and not isinstance(value, python_type):
                raise ContractValidationError(
                    field=field_name,
                    expected=expected_type,
                    actual=value,
                    row_num=row_num,
                )

            # ── Accepted Values Check ──
            accepted = rules.get("accepted_values")
            if accepted and value not in accepted:
                raise ContractValidationError(
                    field=field_name,
                    expected=f"one of {accepted}",
                    actual=value,
                    row_num=row_num,
                )

    @property
    def entity(self) -> str:
        return self._contract["entity"]

    @property
    def version(self) -> str:
        return self._contract["version"]
```

### 3.2 — Tambahkan `pyyaml` ke Dependencies

Edit `ingestion/pyproject.toml`, tambahkan di `dependencies`:

```toml
dependencies = [
    # ... existing dependencies ...
    "pyyaml>=6.0",     # ← tambahkan
]
```

### 3.3 — Integrasikan Validator ke Producer

Edit `ingestion/source/services/kafka_producer_svc.py`:

```python
# Di bagian import, tambahkan:
from source.core.contract_validator import ContractValidator, ContractValidationError

# Di __init__, tambahkan parameter dan method:
def __init__(
    self,
    # ... parameter existing ...
    contract_dir: Optional[str] = None,
) -> None:
    # ... existing init code ...
    self._validators: dict[str, ContractValidator] = {}
    if contract_dir:
        self._load_contracts(Path(contract_dir))

def _load_contracts(self, contract_dir: Path) -> None:
    """Load all YAML contract files from directory."""
    for contract_file in contract_dir.glob("*.yaml"):
        validator = ContractValidator(contract_file)
        self._validators[validator.entity] = validator

# Di _produce_file, dalam loop iter_rows(), tambahkan sebelum EventEnvelope.create():
for row_num, payload in reader.iter_rows():
    try:
        # ── Contract Validation ──
        if entity_type in self._validators:
            self._validators[entity_type].validate(payload, row_num=row_num)

        envelope = EventEnvelope.create(...)
        # ... rest of existing code ...
```

### 3.4 — Unit Test untuk ContractValidator

File baru: `ingestion/tests/unit/test_contract_validator.py`

```python
import tempfile
from pathlib import Path
import pytest
from source.core.contract_validator import ContractValidator, ContractValidationError

VALID_ORDER_CONTRACT = """
entity: order
version: "1.0.0"
owner: data-engineering-team
fields:
  - name: order_id
    type: string
    nullable: false
  - name: order_status
    type: string
    nullable: false
    accepted_values: [delivered, shipped, canceled]
  - name: optional_notes
    type: string
    nullable: true
"""


class TestContractValidator:

    @pytest.fixture
    def contract_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(VALID_ORDER_CONTRACT)
            path = f.name
        yield path
        Path(path).unlink(missing_ok=True)

    def test_valid_payload_passes(self, contract_file):
        validator = ContractValidator(contract_file)
        # Should not raise
        validator.validate({
            "order_id": "ORD-001",
            "order_status": "delivered",
        }, row_num=1)

    def test_null_required_field_fails(self, contract_file):
        validator = ContractValidator(contract_file)
        with pytest.raises(ContractValidationError) as exc:
            validator.validate({"order_id": None, "order_status": "delivered"}, row_num=2)
        assert "order_id" in str(exc.value)
        assert "Row #2" in str(exc.value)

    def test_invalid_accepted_value_fails(self, contract_file):
        validator = ContractValidator(contract_file)
        with pytest.raises(ContractValidationError) as exc:
            validator.validate({
                "order_id": "ORD-001",
                "order_status": "unknown_status",
            }, row_num=3)
        assert "order_status" in str(exc.value)
        assert "delivered" in str(exc.value)

    def test_optional_null_field_passes(self, contract_file):
        validator = ContractValidator(contract_file)
        # Should not raise — optional_notes is nullable
        validator.validate({
            "order_id": "ORD-001",
            "order_status": "shipped",
            "optional_notes": None,
        }, row_num=4)
```

### 3.5 — Jalankan Validasi

```bash
cd ingestion
uv sync  # install pyyaml
uv run pytest tests/unit/test_contract_validator.py -v
```

---

## 4. Implement Incremental Models untuk dbt

**Problem**: Semua model dimaterialisasi sebagai `table` (full rebuild).

### 4.1 — Update `fct_orders` ke Incremental

Edit `transformation/dbt_project/models/marts/fct_orders.sql`:

```sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      "field": "order_purchase_timestamp",
      "data_type": "timestamp",
      "granularity": "month"
    },
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    -- Hanya proses order baru yang masuk sejak last run
    where order_purchase_timestamp >= (
        select max(order_purchase_timestamp) from {{ this }}
    )
    {% endif %}
)

select
    order_id,
    {{ mask_pii('customer_id') }} as customer_id,
    {{ mask_pii('customer_unique_id') }} as customer_unique_id,
    customer_city,
    customer_state,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    total_payment_sequences,
    total_payment_value,
    max_payment_installments,
    avg_review_score,
    total_reviews,
    delivery_time_days,
    delta_delivery_vs_estimated_days
from enriched_orders
```

### 4.2 — Update `fct_order_items` ke Incremental

Edit `transformation/dbt_project/models/marts/fct_order_items.sql`:

```sql
{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with source_items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    -- Order items tidak punya timestamp sendiri, join ke orders
    where order_id in (
        select order_id from {{ ref('fct_orders') }}
    )
    {% endif %}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    price + freight_value as total_item_value
from source_items
```

### 4.3 — Mart Tables: Gunakan `ref()` ke Fact Tables

Pastikan `mart_category_performance` dan `mart_seller_performance` melakukan `ref()` ke fact/dimension tables, bukan langsung ke staging.

Edit `transformation/dbt_project/models/marts/mart_category_performance.sql`:

```sql
{{
  config(
    materialized='table'
  )
}}

select
    p.product_category_name,
    count(distinct oi.order_id) as total_orders,
    sum(oi.order_item_id) as total_items_sold,
    sum(oi.total_item_value) as total_revenue,
    avg(oi.price) as average_item_price
from {{ ref('fct_order_items') }} oi
join {{ ref('dim_products') }} p
    on oi.product_id = p.product_id
group by p.product_category_name
```

### 4.4 — Update `dbt_project.yml`

Edit `transformation/dbt_project/dbt_project.yml`:

```yaml
models:
  lakehouse_transformation:
    +persist_docs:
      relation: true
      columns: true

    staging:
      +materialized: view
      +schema: staging

    intermediate:
      +materialized: ephemeral

    marts:
      # Default: table (untuk dimensi kecil + mart)
      # Fact tables override via config() di file SQL
      +materialized: table
```

### 4.5 — Verifikasi

```bash
cd transformation/dbt_project

# Full refresh pertama kali
uv run dbt run --profiles-dir . --target bigquery --full-refresh

# Incremental run (hanya data baru)
uv run dbt run --profiles-dir . --target bigquery

# Cek bahwa incremental model tidak rebuild seluruh tabel
uv run dbt run --profiles-dir . --target bigquery
# Output seharusnya: "1 of X PASS" atau "NO NEW DATA" untuk model incremental
```

---

## 5. Hapus Secrets & Hardcoded IDs

**Problem**: Credentials dan Project ID tersebar di dokumentasi dan kode.

### 5.1 — Bersihkan README.md

Di `README.md`, ganti semua credential dengan placeholder:

**Sebelum** (baris 477-478):
```bash
$env:SNOWFLAKE_ACCOUNT="GHVRUEH-TX23081"
$env:SNOWFLAKE_USER="RENDAKS"
$env:SNOWFLAKE_PASSWORD="Your_Password_Here"
```

**Sesudah**:
```bash
$env:SNOWFLAKE_ACCOUNT="<YOUR_SNOWFLAKE_ACCOUNT>"
$env:SNOWFLAKE_USER="<YOUR_SNOWFLAKE_USER>"
$env:SNOWFLAKE_PASSWORD="<YOUR_SNOWFLAKE_PASSWORD>"
```

### 5.2 — Hapus Hardcoded Project ID dari benchmark.py

Di `transformation/dbt_project/benchmark.py`, buat project ID menjadi configurable.

**Tambahkan di awal file** (setelah imports):
```python
# Ganti hardcoded project ID dengan environment variable
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your-gcp-project-id")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "LAKEHOUSE_WH")
```

**Ganti semua kemunculan** `project-aaa919f1-4345-401b-860` dengan `{BQ_PROJECT_ID}` dan `COMPUTE_WH` dengan `{SF_WAREHOUSE}`. Gunakan fungsi builder:

```python
def _build_queries(bq_project_id: str) -> dict:
    """Build query dictionary dengan project ID yang di-inject."""
    BQ = bq_project_id  # alias for readability
    return {
        "fct_orders_full_scan": {
            "bq": f"SELECT COUNT(*) as cnt, SUM(total_payment_value) as sum_val, AVG(delivery_time_days) as avg_days FROM `{BQ}.ecommerce_gold.fct_orders`",
            "sf": "SELECT COUNT(*) as cnt, SUM(total_payment_value) as sum_val, AVG(delivery_time_days) as avg_days FROM LAKEHOUSE_RAW.GOLD.FCT_ORDERS"
        },
        # ... semua query lainnya dengan pattern yang sama ...
    }
```

### 5.3 — Hapus `profiles.yml` dari Git Tracking

```bash
cd transformation/dbt_project

# Hapus dari Git index tapi simpan di lokal
git rm --cached profiles.yml

# Pastikan .gitignore sudah benar
# Harus ada baris: **/profiles.yml
```

### 5.4 — Tambahkan `.env.example` yang Lebih Jelas

Update `ingestion/.env.example`:

```ini
# ============================================
# Infisical Configuration
# ============================================
INFISICAL_URL=https://app.infisical.com
INFISICAL_MACHINE_ID=<YOUR_MACHINE_ID>
INFISICAL_MACHINE_SECRET=<YOUR_MACHINE_SECRET>
INFISICAL_PROJECT_ID=<YOUR_PROJECT_ID>

# ============================================
# App Environment
# ============================================
APP_ENV=dev

# ============================================
# GCP Configuration
# ============================================
GCP_PROJECT_ID=<YOUR_GCP_PROJECT_ID>
GCP_SERVICE_ACCOUNT_JSON=<YOUR_SERVICE_ACCOUNT_JSON>
GCS_BRONZE_BUCKET=<YOUR_BRONZE_BUCKET>

# ============================================
# Kafka Configuration
# ============================================
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 5.5 — Verifikasi Tidak Ada Lagi Secrets

```bash
# Cari hardcoded project ID di seluruh repo
rg "project-aaa9191f" --type-add 'all:*' -t all .

# Cari potensi password
rg -i "password\s*=" --glob '!*.lock' --glob '!.git/*' .

# Cari account ID Snowflake
rg "GHVRUEH" .
rg "RENDAKS" .
```

### 5.6 — Rotate Credentials yang Sudah Terekspos

Karena credentials sudah terlanjur muncul di git history:

1. Ganti password Snowflake
2. Regenerate Infisical Machine Secret
3. Rotate GCP Service Account Key
4. Force push jika menggunakan private repo

---

## 📅 Urutan Pengerjaan yang Direkomendasikan

| Prioritas | Improvement | Estimasi | Dependensi |
|---|---|---|---|
| **1** | Hapus secrets & hardcoded IDs | 1-2 jam | Tidak ada |
| **2** | Fix CI/CD pipeline | 1 jam | Tidak ada |
| **3** | Tambahkan real test coverage | 4-6 jam | Tidak ada |
| **4** | Enforce data contracts | 3-4 jam | Nomor 3 (butuh test) |
| **5** | Incremental dbt models | 2-3 jam | Tidak ada |

**Total estimasi**: 11-16 jam (bisa dikerjakan bertahap dalam seminggu)

---

## 📊 Dampak Setelah Perbaikan

| Dimensi | Sebelum | Sesudah |
|---|---|---|
| **CI/CD** | flake8 + requirements.txt (mismatch) | ruff + uv (match dengan tools aktual) |
| **Test Coverage** | Placeholder `assertTrue(True)` | Unit test untuk core services |
| **Data Contracts** | Hanya dokumentasi YAML | Validasi runtime + DLQ routing |
| **dbt Efficiency** | Full table rebuild setiap run | Incremental merge (hanya data baru) |
| **Security** | Credentials di git history | Bersih, semua pakai env vars |
