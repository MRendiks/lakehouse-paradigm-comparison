import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from loguru import logger

class AuditService(ABC):
    """Interface untuk logging audit pipeline agar cloud-portable"""
    @abstractmethod
    def log_run(
        self,
        run_id: str,
        pipeline_stage: str,
        entity_type: str,
        source_topic: Optional[str],
        rows_processed: int,
        rows_failed: int,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        pass

class BigQueryAuditService(AuditService):
    """Adapter audit log untuk Google BigQuery."""
    def __init__(
        self,
        project_id: str,
        dataset_id: str = "ecommerce_gold",
        table_id: str = "pipeline_audit_log",
        credentials_json: Optional[str] = None,
    ) -> None:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        if credentials_json:
            info = json.loads(credentials_json)
            creds = service_account.Credentials.from_service_account_info(info)
            self._client = bigquery.Client(project=project_id, credentials=creds)
        else:
            self._client = bigquery.Client(project=project_id)

        self._table_ref = f"{project_id}.{dataset_id}.{table_id}"
        logger.debug(f"BigQueryAuditService diinisialisasi untuk tabel: {self._table_ref}")

    def log_run(
        self,
        run_id: str,
        pipeline_stage: str,
        entity_type: str,
        source_topic: Optional[str],
        rows_processed: int,
        rows_failed: int,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        row = {
            "run_id": run_id,
            "pipeline_stage": pipeline_stage,
            "entity_type": entity_type,
            "source_topic": source_topic,
            "rows_processed": rows_processed,
            "rows_failed": rows_failed,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "status": status,
            "error_message": error_message,
        }

        try:
            errors = self._client.insert_rows_json(self._table_ref, [row])
            if errors:
                logger.error(f"Gagal mengirim audit log ke BigQuery: {errors}")
            else:
                logger.success(f"Audit log sukses dicatat untuk {entity_type} ({pipeline_stage})")
        except Exception as exc:
            logger.error(f"Kesalahan menulis pipeline_audit_log ke BigQuery: {exc}")
