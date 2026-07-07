import pytest
from source.mapper.handler.topic_router import TopicRouter
from source.core.constants import OlistTopic


class TestTopicRouter:
    """Test CSV filename → Kafka topic resolution."""

    def test_resolve_known_filename(self):
        router = TopicRouter()
        result = router.resolve("olist_orders_dataset.csv")

        assert result is not None
        assert result["topic"] == OlistTopic.ORDERS
        assert result["entity_type"] == "order"

    def test_resolve_all_mapped_files(self):
        """Every registered filename should resolve."""
        router = TopicRouter()
        mappings = {
            "olist_orders_dataset.csv": "order",
            "olist_order_items_dataset.csv": "order_item",
            "olist_order_payments_dataset.csv": "payment",
            "olist_order_reviews_dataset.csv": "review",
            "olist_customers_dataset.csv": "customer",
            "olist_products_dataset.csv": "product",
            "olist_product_category_name_translation.csv": "product_category",
            "olist_sellers_dataset.csv": "seller",
            "olist_geolocation_dataset.csv": "geolocation",
        }

        for filename, expected_entity in mappings.items():
            result = router.resolve(filename)
            assert result is not None, f"Failed to resolve: {filename}"
            assert result["entity_type"] == expected_entity

    def test_unknown_filename_returns_none(self):
        router = TopicRouter()
        result = router.resolve("unknown_file.csv")
        assert result is None

    def test_resolve_or_raise_for_known_file(self):
        router = TopicRouter()
        result = router.resolve_or_raise("olist_orders_dataset.csv")
        assert result["entity_type"] == "order"

    def test_resolve_or_raise_throws_for_unknown(self):
        router = TopicRouter()
        with pytest.raises(ValueError):
            router.resolve_or_raise("nonexistent.csv")