"""
Responsibility:
    Consume messages from a single Kafka topic, accumulate them into batches,
    and upload each batch as a JSON file to GCS under the bronze layer.

Design decisions:
    - One consumer instance = one topic = one entity type.
      This ensures independent offset tracking and cleaner lineage.
    - Batching strategy: accumulate `batch_size` messages in memory, then
      flush to GCS as a single JSON file. This balances GCS request costs
      against latency. File size is predictable regardless of message rate.
    - Hive-style partitioning: bronze/{entity_type}/year={Y}/month={M}/day={D}/
      This allows Spark/BigQuery/dbt to use partition pruning automatically.
    - Each batch file is named with a UUID so concurrent consumers (if scaled
      horizontally) never overwrite each other.
    - On graceful shutdown (KeyboardInterrupt / SIGTERM), any buffered messages
      in the current partial batch are flushed to GCS before exit. No data loss.
    - consumer_group_id is scoped per entity_type so each entity has its own
      committed offsets. Replaying one entity never affects others.

Dependencies:
    - source.services.storage_svc.StorageService (injected — provider-agnostic)
    - confluent_kafka.Consumer
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from loguru import logger


class KafkaToGcsConsumerService:
    """
    Consumes messages from a Kafka topic and uploads batched JSON files to GCS.

    Usage:
        svc = KafkaToGcsConsumerService(
            kafka_bootstrap="localhost:9092",
            topic="ecommerce.olist.orders.v1",
            entity_type="order",
            storage=GCSStorageService(project_id="my-project"),
            gcs_bucket="bronze-lakehouse-my-project",
            batch_size=500,
        )
        svc.run()  # blocking — call in a separate thread/process per topic
    """

    def __init__(
        self,
        kafka_bootstrap: str,
        topic: str,
        entity_type: str,
        storage,  # StorageService instance — injected, provider-agnostic
        gcs_bucket: str,
        batch_size: int = 500,
        poll_timeout: float = 1.0,
        max_wait_time: float = 30.0,
        consumer_group_id: Optional[str] = None,
    ) -> None:
        from confluent_kafka import Consumer

        self._topic = topic
        self._entity_type = entity_type
        self._storage = storage
        self._gcs_bucket = gcs_bucket
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout
        self._max_wait_time = max_wait_time
        self._group_id = consumer_group_id or f"gcs-bronze-{entity_type}"

        self._consumer = Consumer(
            {
                "bootstrap.servers": kafka_bootstrap,
                "group.id": self._group_id,
                # Only read new messages — use 'earliest' for full replay
                "auto.offset.reset": "earliest",
                # Disable auto-commit: we commit only after a successful GCS upload
                # so Kafka offset = GCS file existence. Exactly-once semantics.
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])

        logger.info(
            f"KafkaToGcsConsumerService initialized | topic={topic} | "
            f"entity={entity_type} | batch_size={batch_size} | "
            f"max_wait={max_wait_time}s | group={self._group_id}"
        )

    def run(self) -> None:
        """
        Start the consume loop. Blocking — runs until KeyboardInterrupt or SIGTERM.

        Flow per iteration:
            poll(timeout) → accumulate into buffer
            if buffer >= batch_size OR time elapsed >= max_wait_time:
                flush_to_gcs() → commit offsets → clear buffer
        """
        import time

        buffer: list[dict] = []
        last_flush_time = time.time()
        logger.info(
            f"Starting consumer loop | topic={self._topic} | "
            f"max_batch={self._batch_size} | max_wait={self._max_wait_time}s"
        )

        try:
            while True:
                msg = self._consumer.poll(timeout=self._poll_timeout)

                if msg is not None:
                    if msg.error():
                        self._handle_kafka_error(msg.error())
                    else:
                        parsed = self._parse_message(msg)
                        if parsed is not None:
                            buffer.append(parsed)

                # Check if we should flush
                size_reached = len(buffer) >= self._batch_size
                time_reached = (
                    time.time() - last_flush_time >= self._max_wait_time
                ) and len(buffer) > 0

                if size_reached or time_reached:
                    reason = "size" if size_reached else "timeout"
                    logger.debug(f"Flushing batch ({reason}) | records={len(buffer)}")
                    self._flush_batch(buffer)
                    self._consumer.commit(asynchronous=False)
                    buffer.clear()
                    last_flush_time = time.time()

        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
        finally:
            # Flush remaining buffered messages before exit — no data loss
            if buffer:
                logger.info(
                    f"Flushing partial batch of {len(buffer)} messages before shutdown."
                )
                self._flush_batch(buffer)
                self._consumer.commit(asynchronous=False)

            self._consumer.close()
            logger.info(f"Consumer closed | topic={self._topic}")

    def _parse_message(self, msg) -> Optional[dict]:
        """Deserialize a Kafka message value from JSON bytes to dict."""
        try:
            return json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.error(
                f"Failed to parse message | topic={self._topic} "
                f"offset={msg.offset()} | error={exc}"
            )
            return None

    def _flush_batch(self, buffer: list[dict]) -> None:
        """
        Serialize buffer → upload to GCS as a single JSON file.

        GCS path format (Hive-style partitions for Spark/BigQuery compatibility):
            bronze/{entity_type}/year={Y}/month={M:02d}/day={D:02d}/batch_{uuid}.json
        """
        now = datetime.now(tz=timezone.utc)
        batch_id = uuid.uuid4().hex[:12]

        gcs_path = (
            f"{self._entity_type}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"batch_{batch_id}.json"
        )

        # NDJSON (Newline-Delimited JSON): one record per line.
        # More memory-efficient for BigQuery External Table ingestion vs. JSON array.
        ndjson_bytes = "\n".join(json.dumps(row, default=str) for row in buffer).encode(
            "utf-8"
        )

        try:
            uri = self._storage.upload_bytes(
                bucket=self._gcs_bucket,
                path=gcs_path,
                data=ndjson_bytes,
                content_type="application/x-ndjson",
            )
            logger.success(
                f"Batch uploaded | entity={self._entity_type} | "
                f"records={len(buffer)} | uri={uri}"
            )
        except Exception as exc:
            logger.error(
                f"GCS upload failed | entity={self._entity_type} | "
                f"batch_id={batch_id} | error={exc}"
            )
            raise  # Re-raise so offsets are NOT committed after a failed upload

    def _handle_kafka_error(self, error) -> None:
        """Handle Kafka consumer errors."""
        from confluent_kafka import KafkaError

        if error.code() == KafkaError._PARTITION_EOF:
            # Reached end of partition — normal during low-traffic periods
            logger.debug(f"Reached end of partition | topic={self._topic}")
        else:
            logger.error(f"Kafka consumer error | topic={self._topic} | error={error}")
