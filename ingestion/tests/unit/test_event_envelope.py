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