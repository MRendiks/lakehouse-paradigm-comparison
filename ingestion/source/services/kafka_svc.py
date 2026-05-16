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
        """Kirim message yang gagal ke Dead Letter Queue."""
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
            logger.info(f"Message dikirim ke DLQ: {OlistTopic.DLQ.value}")
        except KafkaException as dlq_exc:
            logger.critical(f"Gagal kirim ke DLQ: {dlq_exc}")

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
