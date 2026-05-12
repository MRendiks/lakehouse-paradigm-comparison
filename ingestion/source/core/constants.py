from enum import Enum

class DataLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

KAFKA_TOPIC_DEFAULT = "ecommerce-events"
