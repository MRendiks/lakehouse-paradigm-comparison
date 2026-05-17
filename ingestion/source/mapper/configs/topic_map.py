"""
Defines the mapping from CSV filename → (Kafka topic, entity_type).

Why here and not in constants.py?
    - constants.py should contain only enums and primitive constants
      (values with no business routing responsibility).
    - DATASET_TOPIC_MAP is a *business routing rule*: given a source file,
      which topic does it belong to? That is the mapper layer's concern.
    - Keeping it here makes it easy to:
        1. Add new data sources without touching core constants.
        2. Override or extend mapping per environment if needed.
        3. Unit-test routing logic in isolation.

Convention:
    Keys   → exact CSV filename (basename)
    Values → dict with keys:
                "topic"       : OlistTopic enum value
                "entity_type" : singular entity name string (used in event_type & lineage)
"""

from source.core.constants import OlistTopic

DATASET_TOPIC_MAP: dict[str, dict] = {
    "olist_orders_dataset.csv": {
        "topic": OlistTopic.ORDERS,
        "entity_type": "order",
    },
    "olist_order_items_dataset.csv": {
        "topic": OlistTopic.ORDER_ITEMS,
        "entity_type": "order_item",
    },
    "olist_order_payments_dataset.csv": {
        "topic": OlistTopic.PAYMENTS,
        "entity_type": "payment",
    },
    "olist_order_reviews_dataset.csv": {
        "topic": OlistTopic.REVIEWS,
        "entity_type": "review",
    },
    "olist_customers_dataset.csv": {
        "topic": OlistTopic.CUSTOMERS,
        "entity_type": "customer",
    },
    "olist_products_dataset.csv": {
        "topic": OlistTopic.PRODUCTS,
        "entity_type": "product",
    },
    "olist_sellers_dataset.csv": {
        "topic": OlistTopic.SELLERS,
        "entity_type": "seller",
    },
    "olist_geolocation_dataset.csv": {
        "topic": OlistTopic.GEOLOCATION,
        "entity_type": "geolocation",
    },
    "product_category_name_translation.csv": {
        "topic": OlistTopic.PRODUCTS,
        "entity_type": "product_category",
    },
}
