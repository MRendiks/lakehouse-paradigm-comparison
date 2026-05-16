"""
models package — Pydantic models for ingestion layer.

Exports:
    EventEnvelope   — standard wrapper message Kafka (metadata + payload)
    EventMetadata   — metadata section of the envelope
    EcommerceEvent  — legacy model (kept for backward compatibility)
"""

from source.models.event_envelope import EventEnvelope, EventMetadata
from source.models.schema_model import EcommerceEvent

__all__ = [
    "EventEnvelope",
    "EventMetadata",
    "EcommerceEvent",
]
