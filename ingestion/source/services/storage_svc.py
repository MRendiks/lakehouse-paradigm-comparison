"""
storage_svc.py — Cloud Storage Adapter Layer

Menggunakan Adapter Pattern agar pipeline core (Kafka, EventEnvelope, dbt)
tidak perlu tahu cloud provider mana yang digunakan.

Menambah cloud baru = buat subclass baru, tidak menyentuh kode yang ada.

Hierarchy:
    StorageService (ABC)          ← interface contract
        ├── GCSStorageService     ← GCP (current stack)
        ├── S3StorageService      ← AWS (future extension)
        └── ADLSStorageService    ← Azure (future extension)
"""

from abc import ABC, abstractmethod
from loguru import logger


# ──────────────────────────────────────────────────────────────────────────────
# Abstract Base — Interface Contract
# ──────────────────────────────────────────────────────────────────────────────

class StorageService(ABC):
    """
    Interface contract untuk semua cloud storage provider.
    Caller hanya bergantung pada interface ini — tidak tahu provider-nya.
    """

    @abstractmethod
    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str = "application/json") -> str:
        """
        Upload raw bytes ke cloud storage.

        Args:
            bucket: nama bucket/container
            path:   path tujuan dalam bucket (contoh: 'bronze/order/2026/05/17/batch_001.json')
            data:   konten file dalam bytes
            content_type: MIME type

        Returns:
            Full URI dari object yang diupload (contoh: 'gs://bucket/path' atau 's3://bucket/path')
        """

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """
        List semua object dengan prefix tertentu.

        Returns:
            List of object paths (bukan full URI)
        """

    @abstractmethod
    def object_exists(self, bucket: str, path: str) -> bool:
        """Cek apakah object sudah ada di storage."""

    def upload_json(self, bucket: str, path: str, payload: dict) -> str:
        """Convenience method — serialize dict ke JSON bytes lalu upload."""
        import json
        data = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
        return self.upload_bytes(bucket, path, data, content_type="application/json")


# ──────────────────────────────────────────────────────────────────────────────
# GCP Adapter — Google Cloud Storage
# ──────────────────────────────────────────────────────────────────────────────

class GCSStorageService(StorageService):
    """
    Adapter untuk Google Cloud Storage.
    Gunakan ADC (Application Default Credentials) untuk autentikasi lokal.
    Di production/Docker: gunakan service account JSON dari Infisical.
    """

    def __init__(self, project_id: str, credentials_json: str | None = None) -> None:
        """
        Args:
            project_id:       GCP project ID
            credentials_json: opsional — JSON string service account.
                              Jika None, gunakan ADC (gcloud auth application-default login).
        """
        from google.cloud import storage
        from google.oauth2 import service_account
        import json

        if credentials_json:
            info = json.loads(credentials_json)
            creds = service_account.Credentials.from_service_account_info(info)
            self._client = storage.Client(project=project_id, credentials=creds)
        else:
            # ADC — untuk local development
            self._client = storage.Client(project=project_id)

        self._project_id = project_id
        logger.debug(f"GCSStorageService initialized — project={project_id}")

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str = "application/json") -> str:
        blob = self._client.bucket(bucket).blob(path)
        blob.upload_from_string(data, content_type=content_type)
        uri = f"gs://{bucket}/{path}"
        logger.success(f"Uploaded → {uri} ({len(data):,} bytes)")
        return uri

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        blobs = self._client.list_blobs(bucket, prefix=prefix)
        return [b.name for b in blobs]

    def object_exists(self, bucket: str, path: str) -> bool:
        return self._client.bucket(bucket).blob(path).exists()


# ──────────────────────────────────────────────────────────────────────────────
# AWS Adapter — Amazon S3  (Future Extension — Fase 9)
# ──────────────────────────────────────────────────────────────────────────────

class S3StorageService(StorageService):
    """
    Adapter untuk Amazon S3.
    Extension point untuk AWS stack — implementasi sama, hanya provider berbeda.
    Dependency: boto3 (tambahkan ke pyproject.toml saat Fase 9 aktif)
    """

    def __init__(self, region: str, aws_access_key: str = "", aws_secret_key: str = "") -> None:
        import boto3
        session = boto3.Session(
            aws_access_key_id=aws_access_key or None,
            aws_secret_access_key=aws_secret_key or None,
            region_name=region,
        )
        self._client = session.client("s3")
        logger.debug(f"S3StorageService initialized — region={region}")

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str = "application/json") -> str:
        self._client.put_object(Bucket=bucket, Key=path, Body=data, ContentType=content_type)
        uri = f"s3://{bucket}/{path}"
        logger.success(f"Uploaded → {uri} ({len(data):,} bytes)")
        return uri

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        resp = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def object_exists(self, bucket: str, path: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=path)
            return True
        except Exception:
            return False


# ──────────────────────────────────────────────────────────────────────────────
# Azure Adapter — Azure Data Lake Storage Gen2  (Future Extension — Fase 9)
# ──────────────────────────────────────────────────────────────────────────────

class ADLSStorageService(StorageService):
    """
    Adapter untuk Azure Data Lake Storage Gen2.
    Extension point untuk Azure stack.
    Dependency: azure-storage-file-datalake (tambahkan ke pyproject.toml saat Fase 9 aktif)
    """

    def __init__(self, account_name: str, account_key: str) -> None:
        from azure.storage.filedatalake import DataLakeServiceClient
        url = f"https://{account_name}.dfs.core.windows.net"
        self._client = DataLakeServiceClient(account_url=url, credential=account_key)
        self._account = account_name
        logger.debug(f"ADLSStorageService initialized — account={account_name}")

    def upload_bytes(self, bucket: str, path: str, data: bytes, content_type: str = "application/json") -> str:
        # bucket = container name in Azure
        fs = self._client.get_file_system_client(file_system=bucket)
        file_client = fs.get_file_client(path)
        file_client.upload_data(data, overwrite=True)
        uri = f"abfss://{bucket}@{self._account}.dfs.core.windows.net/{path}"
        logger.success(f"Uploaded → {uri} ({len(data):,} bytes)")
        return uri

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        fs = self._client.get_file_system_client(file_system=bucket)
        return [p.name for p in fs.get_paths(path=prefix)]

    def object_exists(self, bucket: str, path: str) -> bool:
        try:
            fs = self._client.get_file_system_client(file_system=bucket)
            fs.get_file_client(path).get_file_properties()
            return True
        except Exception:
            return False


# ──────────────────────────────────────────────────────────────────────────────
# Factory — buat instance berdasarkan config (tidak hardcode provider)
# ──────────────────────────────────────────────────────────────────────────────

def create_storage_service(provider: str, **kwargs) -> StorageService:
    """
    Factory function — pilih storage provider dari config/env.

    Args:
        provider: 'gcs' | 's3' | 'adls'
        **kwargs: argumen spesifik per provider

    Example:
        # GCP (current stack)
        svc = create_storage_service('gcs', project_id='my-project')

        # AWS (Fase 9)
        svc = create_storage_service('s3', region='ap-southeast-1')

        # Azure (Fase 9)
        svc = create_storage_service('adls', account_name='myaccount', account_key='...')
    """
    providers = {
        "gcs":  GCSStorageService,
        "s3":   S3StorageService,
        "adls": ADLSStorageService,
    }
    if provider not in providers:
        raise ValueError(f"Unknown storage provider: '{provider}'. Choose from: {list(providers.keys())}")
    return providers[provider](**kwargs)
