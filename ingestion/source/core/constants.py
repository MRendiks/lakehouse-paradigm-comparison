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
