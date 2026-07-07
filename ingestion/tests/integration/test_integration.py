import os
import pytest


@pytest.mark.integration
class TestIntegration:
    """Integration tests — require running Kafka and GCS emulators."""

    @pytest.mark.skipif(
        not os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        reason="KAFKA_BOOTSTRAP_SERVERS not set — set to run integration tests",
    )
    def test_kafka_connection(self):
        """Verify Kafka broker is reachable and can produce/consume."""
        from confluent_kafka import Producer, Consumer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        test_topic = "test.integration.v1"

        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(test_topic, b'{"test": "hello"}')
        producer.flush(timeout=5)

        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "test-integration-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([test_topic])

        msg = consumer.poll(timeout=5)
        assert msg is not None
        assert not msg.error()

        consumer.close()

    @pytest.mark.skipif(
        not os.getenv("GCS_BRONZE_BUCKET"),
        reason="GCS_BRONZE_BUCKET not set",
    )
    def test_gcs_upload_download(self):
        """Verify GCS upload and object existence."""
        from source.services.storage_svc import GCSStorageService

        project_id = os.getenv("GCP_PROJECT_ID", "test")
        bucket = os.getenv("GCS_BRONZE_BUCKET", "test-bucket")

        svc = GCSStorageService(project_id=project_id)
        test_path = "test/integration_test.json"

        uri = svc.upload_bytes(
            bucket=bucket,
            path=test_path,
            data=b'{"test": true}',
        )
        assert uri.startswith("gs://")