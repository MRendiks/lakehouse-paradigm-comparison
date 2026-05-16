from enum import Enum


class DataLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class OlistTopic(str, Enum):
    """
    Kafka topic naming convention: {domain}.{source}.{entity}.{version}
    Per-entity topic design — standar cloud data engineering.
    """

    ORDERS = "ecommerce.olist.orders.v1"
    ORDER_ITEMS = "ecommerce.olist.order-items.v1"
    PAYMENTS = "ecommerce.olist.payments.v1"
    REVIEWS = "ecommerce.olist.reviews.v1"
    CUSTOMERS = "ecommerce.olist.customers.v1"
    PRODUCTS = "ecommerce.olist.products.v1"
    SELLERS = "ecommerce.olist.sellers.v1"
    GEOLOCATION = "ecommerce.olist.geolocation.v1"
    DLQ = "ecommerce.dlq.v1"


DATASET_TOPIC_MAP: dict[str, dict] = {
    "olist_orders_dataset.csv": {"topic": OlistTopic.ORDERS, "entity_type": "order"},
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
    "olist_sellers_dataset.csv": {"topic": OlistTopic.SELLERS, "entity_type": "seller"},
    "olist_geolocation_dataset.csv": {
        "topic": OlistTopic.GEOLOCATION,
        "entity_type": "geolocation",
    },
    "product_category_name_translation.csv": {
        "topic": OlistTopic.PRODUCTS,
        "entity_type": "product_category",
    },
}

KAFKA_TOPIC_DEFAULT = "ecommerce-events"
