from confluent_kafka import Producer, KafkaException
from loguru import logger

from source.core.constants import OlistTopic
from source.models.event_envelope import EventEnvelope


class KafkaService:
    """
    Kafka producer service — envelope-driven, multi-topic.

    Every message must be wrapped in an EventEnvelope before being produced.
    The target topic is taken directly from envelope.metadata.source_topic,
    making this service topic-agnostic (does not need to know data types).

    DLQ (Dead Letter Queue) is automatically used when produce fails.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",  # wait for all replica confirmations
                "retries": 3,
                "retry.backoff.ms": 300,
                "enable.idempotence": True,  # exactly-once semantics
            }
        )

    def produce(self, envelope: EventEnvelope) -> None:
        """
        Produce satu EventEnvelope ke topic yang sesuai.
        If failed, automatically sent to DLQ.

        Args:
            envelope: EventEnvelope berisi metadata + payload
        """
        topic = envelope.metadata.source_topic
        key = envelope.kafka_key()
        value = envelope.to_kafka_message()

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=self._serialize(value),
                on_delivery=self._delivery_report,
            )
            self._producer.poll(0)  # trigger async callback tanpa blocking
            logger.debug(
                f"Produced → topic={topic} key={key} entity={envelope.metadata.entity_type}"
            )
        except KafkaException as exc:
            logger.error(f"Produce failed → {exc}. Routing to DLQ.")
            self._send_to_dlq(envelope, error=str(exc))

    def flush(self, timeout: float = 30.0) -> None:
        """Wait for all outstanding messages to be sent before shutting down."""
        pending = self._producer.flush(timeout=timeout)
        if pending > 0:
            logger.warning(
                f"{pending} message(s) belum terkirim setelah flush timeout."
            )

    def _send_to_dlq(self, envelope: EventEnvelope, error: str) -> None:
        """
        Send a failed EventEnvelope to the Dead Letter Queue.
        Used when Kafka produce() itself raises KafkaException.
        """
        dlq_payload = {
            "original_topic": envelope.metadata.source_topic,
            "error": error,
            "envelope": envelope.to_kafka_message(),
        }
        try:
            self._producer.produce(
                topic=OlistTopic.DLQ.value,
                key=envelope.kafka_key().encode("utf-8"),
                value=self._serialize(dlq_payload),
            )
            self._producer.poll(0)
            logger.warning(
                f"DLQ ← envelope routed | topic={OlistTopic.DLQ.value} | error={error}"
            )
        except KafkaException as dlq_exc:
            logger.critical(f"Failed to send to DLQ: {dlq_exc}")

    def produce_raw_to_dlq(
        self,
        raw_payload: dict,
        original_topic: str,
        error: str,
        row_num: int,
        filename: str,
    ) -> None:
        """
        Send a raw (pre-envelope) payload to the DLQ.
        Used when EventEnvelope.create() itself fails — no envelope object available.

        Args:
            raw_payload:    The original CSV row dict that failed processing.
            original_topic: The intended target topic (string value).
            error:          Exception message explaining why the row failed.
            row_num:        1-based row number from the source CSV file.
            filename:       Source CSV filename for traceability.
        """
        import json

        dlq_payload = {
            "original_topic": original_topic,
            "source_file": filename,
            "row_num": row_num,
            "error": error,
            "raw_payload": raw_payload,
        }
        try:
            key = f"{filename}:row:{row_num}".encode("utf-8")
            self._producer.produce(
                topic=OlistTopic.DLQ.value,
                key=key,
                value=self._serialize(dlq_payload),
            )
            self._producer.poll(0)
            logger.warning(
                f"DLQ ← raw payload routed | file={filename} | "
                f"row={row_num} | error={error}"
            )
        except KafkaException as dlq_exc:
            logger.critical(
                f"Failed to send raw payload to DLQ: {dlq_exc} | "
                f"file={filename} | row={row_num}"
            )

    @staticmethod
    def _serialize(data: dict) -> bytes:
        import json

        return json.dumps(data, default=str).encode("utf-8")

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err:
            logger.error(f"Delivery failed → topic={msg.topic()} err={err}")
        else:
            logger.success(
                f"Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )
