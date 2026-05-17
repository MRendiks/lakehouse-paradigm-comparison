"""
mapper/handler/topic_router.py — Topic Routing Logic

Resolves which Kafka topic and entity_type to use for a given CSV file.

Separated from topic_map.py so that:
    - topic_map.py stays a pure data config (no logic)
    - topic_router.py contains the resolution/lookup logic (testable in isolation)
"""

from pathlib import Path
from typing import Optional

from source.mapper.configs.topic_map import DATASET_TOPIC_MAP
from source.core.constants import OlistTopic


class TopicRouter:
    """
    Resolves CSV filename → (topic, entity_type) using DATASET_TOPIC_MAP.

    Usage:
        router = TopicRouter()
        config = router.resolve("olist_orders_dataset.csv")
        # → {"topic": OlistTopic.ORDERS, "entity_type": "order"}
    """

    def resolve(self, filename: str) -> Optional[dict]:
        """
        Look up routing config by CSV filename (basename only).

        Args:
            filename: basename of the CSV file, e.g. 'olist_orders_dataset.csv'

        Returns:
            dict with 'topic' (OlistTopic) and 'entity_type' (str),
            or None if no mapping is registered for this file.
        """
        key = Path(filename).name
        return DATASET_TOPIC_MAP.get(key)

    def resolve_or_raise(self, filename: str) -> dict:
        """
        Same as resolve(), but raises ValueError if no mapping found.
        Use this when the caller cannot proceed without a valid topic.
        """
        config = self.resolve(filename)
        if config is None:
            registered = list(DATASET_TOPIC_MAP.keys())
            raise ValueError(
                f"No topic mapping found for '{filename}'. "
                f"Registered files: {registered}"
            )
        return config

    def list_registered_files(self) -> list[str]:
        """Return all CSV filenames that have a registered topic mapping."""
        return list(DATASET_TOPIC_MAP.keys())

    def list_registered_topics(self) -> list[OlistTopic]:
        """Return all unique OlistTopic values currently in use."""
        return list({cfg["topic"] for cfg in DATASET_TOPIC_MAP.values()})
