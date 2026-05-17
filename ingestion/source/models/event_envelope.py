import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

from source.core.constants import DataLayer, OlistTopic


class EventMetadata(BaseModel):
    """
    A standard metadata envelope for every Kafka message. This metadata is independent of the payload content,
    allowing consumers to perform routing, deduplication, and lineage tracking without needing to read the payload.
    """

    # --- Event unique identification ---
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="UUID v4 — used for idempotent processing & deduplication",
    )

    # --- Routing & Classification ---
    event_type: str = Field(
        description="Format: '{source}.{entity}.{action}' — example: 'olist.order.created'",
    )
    entity_type: str = Field(
        description="Entity name without version — example: 'order', 'customer', 'payment'",
    )

    # --- Provenance (source of data) ---
    source_system: str = Field(
        default="olist-csv-producer",
        description="Name of the service/producer that generates the event",
    )
    source_topic: str = Field(
        description="Full topic name — useful for tracing when the message enters the DLQ",
    )

    # --- Schema management ---
    schema_version: str = Field(
        default="1.0.0",
        description="Semantic versioning schema payload — for backward compatibility",
    )

    # --- Timestamp ---
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="Time when the data was ingested by the producer (not the original event time)",
    )

    # --- Environment & pipeline ---
    ingestion_env: str = Field(
        default="dev",
        description="Environment: 'dev' | 'staging' | 'prod'",
    )
    pipeline_run_id: Optional[str] = Field(
        default=None,
        description="ID batch run — for tracking per pipeline execution",
    )
    data_layer: str = Field(
        default=DataLayer.BRONZE.value,
        description="Lakehouse layer: 'bronze' | 'silver' | 'gold'",
    )

    # --- Data Integrity ---
    checksum: Optional[str] = Field(
        default=None,
        description="SHA-256 hash of the payload — for integrity validation",
    )


class EventEnvelope(BaseModel):
    """
    Standar wrapper untuk semua Kafka message di pipeline Olist E-Commerce.

    Setiap message yang diproduce ke Kafka harus menggunakan envelope ini
    agar consumer dapat melakukan:
    - Routing berdasarkan entity_type tanpa membaca payload
    - Idempotent processing via event_id
    - Schema evolution management via schema_version
    - Lineage tracing via pipeline_run_id & source_topic
    """

    metadata: EventMetadata
    payload: dict[str, Any]

    @classmethod
    def create(
        cls,
        topic: OlistTopic,
        entity_type: str,
        payload: dict[str, Any],
        pipeline_run_id: Optional[str] = None,
        ingestion_env: str = "dev",
        source_system: str = "olist-csv-producer",
    ) -> "EventEnvelope":
        """
        Factory method — creates an EventEnvelope by automatically populating the metadata.

        Args:
            topic: OlistTopic enum — destination topic (used for source_topic & event_type)
            entity_type: entity name, e.g., ‘order’, ‘customer’
            payload: raw data from a CSV row (dict)
            pipeline_run_id: optional ID for batch tracking
            ingestion_env: target environment (‘dev’, ‘staging’, ‘prod’)
            source_system: producer service name

        Returns:
            EventEnvelope ready to be produced to Kafka

        Example:
            >>> envelope = EventEnvelope.create(
            ...     topic=OlistTopic.ORDERS,
            ...     entity_type="order",
            ...     payload={“order_id”: “abc123”, ‘status’: “delivered”},
            ...     pipeline_run_id="run-2026-05-16",
            ... )
        """
        payload_checksum = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()

        event_type = f"olist.{entity_type}.created"

        metadata = EventMetadata(
            event_type=event_type,
            entity_type=entity_type,
            source_system=source_system,
            source_topic=topic.value,
            ingestion_env=ingestion_env,
            pipeline_run_id=pipeline_run_id,
            checksum=payload_checksum,
        )

        return cls(metadata=metadata, payload=payload)

    def to_kafka_message(self) -> dict[str, Any]:
        """Serialize envelope to dict — ready to be passed to Kafka producer."""
        return self.model_dump(mode="json")

    def kafka_key(self) -> str:
        """
        Kafka message key — used for partitioning.
        Priority: take the *_id field from the payload, fallback to event_id.
        Key consistency ensures that events from the same entity
        enter the same partition (ordering guarantee).
        """
        entity = self.metadata.entity_type
        # try various possible primary key names
        for key_candidate in [f"{entity}_id", "id", "order_id", "customer_id"]:
            if key_candidate in self.payload and self.payload[key_candidate]:
                return str(self.payload[key_candidate])
        return self.metadata.event_id
