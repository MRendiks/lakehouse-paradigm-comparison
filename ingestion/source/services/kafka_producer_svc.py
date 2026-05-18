"""
Responsibility:
    Orchestrate the full ingestion flow:
        CsvReader.iter_rows() → EventEnvelope.create() → KafkaService.produce()

Design decisions:
    - CSV reading and normalization are fully delegated to CsvReader (mapper layer).
      This service has zero knowledge of file formats or data cleansing rules.
    - Topic routing is fully automatic via TopicRouter (no hardcoding in this layer).
    - Each CSV row is wrapped in an EventEnvelope before being sent to Kafka.
    - Failures per-row are caught individually so one bad row does not abort the batch.
    - The pipeline_run_id ties every message in a batch run together for lineage tracking.
    - flush() is always called at the end so no messages are silently dropped.

Dependencies:
    - source.mapper.handler.csv_reader.CsvReader
    - source.mapper.handler.topic_router.TopicRouter
    - source.models.event_envelope.EventEnvelope
    - source.services.kafka_svc.KafkaService
"""

import datetime
import uuid
from datetime import timezone
from pathlib import Path
from typing import Optional
from loguru import logger

from source.services.audit_svc import BigQueryAuditService
from source.config.settings import settings
from source.mapper.handler.csv_reader import CsvReader
from source.mapper.handler.topic_router import TopicRouter
from source.models.event_envelope import EventEnvelope
from source.services.kafka_svc import KafkaService


class CsvKafkaProducerService:
    """
    Reads one or more CSV files and produces each row as an EventEnvelope to Kafka.

    Usage:
        svc = CsvKafkaProducerService(
            kafka_bootstrap="localhost:9092",
            data_dir=Path("/data/olist"),
            ingestion_env="dev",
        )
        stats = svc.run()
    """

    def __init__(
        self,
        kafka_bootstrap: str,
        data_dir: Path,
        ingestion_env: str = "dev",
        pipeline_run_id: Optional[str] = None,
        source_system: str = "olist-csv-producer",
        flush_timeout: float = 30.0,
    ) -> None:
        self._kafka = KafkaService(bootstrap_servers=kafka_bootstrap)
        self._router = TopicRouter()
        self._data_dir = Path(data_dir)
        self._ingestion_env = ingestion_env
        self._pipeline_run_id = pipeline_run_id or f"run-{uuid.uuid4().hex[:8]}"
        self._source_system = source_system
        self._flush_timeout = flush_timeout

        self._audit_svc = BigQueryAuditService(
            project_id=settings.GCP_PROJECT_ID,
            credentials_json=settings.GCP_SERVICE_ACCOUNT_JSON or None,
        )

        logger.info(
            f"CsvKafkaProducerService initialized | "
            f"env={ingestion_env} | run_id={self._pipeline_run_id} | "
            f"data_dir={self._data_dir}"
        )

    def run(self) -> dict:
        """
        Entry point — scan data_dir, produce all mapped CSV files to Kafka.

        Returns:
            Summary stats dict: {filename: {"produced": int, "failed": int}}
        """
        stats: dict[str, dict] = {}
        csv_files = self._discover_csv_files()

        if not csv_files:
            logger.warning(f"No CSV files found in {self._data_dir}")
            return stats

        for csv_path in csv_files:
            filename = csv_path.name
            topic_config = self._router.resolve(filename)

            if topic_config is None:
                logger.warning(
                    f"No topic mapping found for '{filename}' — skipping. "
                    f"Register it in mapper/configs/topic_map.py to enable ingestion."
                )
                continue

            file_stats = self._produce_file(
                csv_path=csv_path,
                topic=topic_config["topic"],
                entity_type=topic_config["entity_type"],
            )
            stats[filename] = file_stats

        # Guarantee delivery before shutdown
        self._kafka.flush(timeout=self._flush_timeout)
        self._log_summary(stats)
        return stats

    def run_single_file(self, filename: str) -> dict:
        """
        Produce a single CSV file by name (useful for targeted re-ingestion).

        Args:
            filename: basename of the CSV file, e.g. 'olist_orders_dataset.csv'
        """
        csv_path = self._data_dir / filename
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        topic_config = self._router.resolve_or_raise(filename)

        stats = self._produce_file(
            csv_path=csv_path,
            topic=topic_config["topic"],
            entity_type=topic_config["entity_type"],
        )
        self._kafka.flush(timeout=self._flush_timeout)
        return stats

    # Private helpers
    def _discover_csv_files(self) -> list[Path]:
        """Return all CSV files under data_dir sorted by name for deterministic ordering."""
        if not self._data_dir.is_dir():
            logger.error(
                f"data_dir does not exist or is not a directory: {self._data_dir}"
            )
            return []
        return sorted(self._data_dir.glob("*.csv"))

    def _produce_file(self, csv_path: Path, topic, entity_type: str) -> dict:
        """
        Produce all rows from a CSV file to Kafka as EventEnvelopes.

        CSV reading and normalization are fully delegated to CsvReader.
        This method only knows about: enveloping → producing → counting.

        Args:
            csv_path:    absolute path to the CSV file
            topic:       OlistTopic enum value for this file
            entity_type: entity name string, e.g. 'order'

        Returns:
            {"produced": int, "failed": int}
        """
        started_at = datetime.datetime.now(timezone.utc)
        produced = 0
        failed = 0
        reader = CsvReader(csv_path)

        logger.info(
            f"Starting ingestion | file={reader.filename} | "
            f"topic={topic.value} | entity={entity_type}"
        )

        for row_num, payload in reader.iter_rows():
            try:
                envelope = EventEnvelope.create(
                    topic=topic,
                    entity_type=entity_type,
                    payload=payload,
                    pipeline_run_id=self._pipeline_run_id,
                    ingestion_env=self._ingestion_env,
                    source_system=self._source_system,
                )
                self._kafka.produce(envelope)
                produced += 1

            except Exception as exc:
                logger.error(
                    f"Row #{row_num} failed | file={reader.filename} | error={exc}"
                )
                self._kafka.produce_raw_to_dlq(
                    raw_payload=payload,
                    original_topic=topic.value,
                    error=str(exc),
                    row_num=row_num,
                    filename=reader.filename,
                )
                failed += 1

        finished_at = datetime.datetime.now(timezone.utc)
        status = "success" if failed == 0 else "partial" if produced > 0 else "failed"

        try:
            self._audit_svc.log_run(
                run_id=self._pipeline_run_id,
                pipeline_stage="ingestion",
                entity_type=entity_type,
                source_topic=topic.value,
                rows_processed=produced,
                rows_failed=failed,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                error_message=None if failed == 0 else f"Ingestion failed for {failed} rows.",
            )
        except Exception as audit_exc:
            logger.error(f"Gagal mencatat audit log: {audit_exc}")

        logger.success(
            f"Finished ingestion | file={reader.filename} | "
            f"produced={produced} | failed={failed}"
        )
        return {"produced": produced, "failed": failed}

    @staticmethod
    def _log_summary(stats: dict) -> None:
        """Print a formatted summary table after all files are processed."""
        total_produced = sum(v["produced"] for v in stats.values())
        total_failed = sum(v["failed"] for v in stats.values())

        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        for filename, counts in stats.items():
            status = "✓" if counts["failed"] == 0 else "✗"
            logger.info(
                f"  [{status}] {filename:<45} "
                f"produced={counts['produced']:>6}  failed={counts['failed']:>4}"
            )
        logger.info("-" * 60)
        logger.info(f"  TOTAL: produced={total_produced}  failed={total_failed}")
        logger.info("=" * 60)
