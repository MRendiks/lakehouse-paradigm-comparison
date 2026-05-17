"""
Responsibility:
    Parse CLI arguments and delegate execution to KafkaToGcsConsumerService.
    This file contains ZERO business logic — it is a pure CLI adapter.

Commands:
    stream   Subscribe to a single Kafka topic and stream messages to GCS bronze.

Usage:
    ingestion-run consumer stream --topic ecommerce.olist.orders.v1 --entity order --bucket bronze-lakehouse-xxx
    ingestion-run consumer stream --topic ecommerce.olist.orders.v1 --entity order --bucket bronze-lakehouse-xxx --batch-size 1000
"""

import sys
import typer

app = typer.Typer(help="Kafka Consumer → GCS Bronze Layer")


@app.command()
def stream(
    topic: str = typer.Option(
        ...,
        "--topic",
        "-t",
        help="Full Kafka topic name, e.g. 'ecommerce.olist.orders.v1'",
    ),
    entity: str = typer.Option(
        ...,
        "--entity",
        "-e",
        help="Entity type name for GCS path partitioning, e.g. 'order'",
    ),
    bucket: str = typer.Option(
        None,
        "--bucket",
        "-b",
        help="GCS bucket name for bronze layer (without gs:// prefix)",
        envvar="GCS_BRONZE_BUCKET",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        help="Target environment for secret resolution: dev | staging | prod",
        envvar="APP_ENV",
    ),
    batch_size: int = typer.Option(
        500,
        "--batch-size",
        help="Number of messages to accumulate before uploading a single GCS file.",
    ),
    group_id: str = typer.Option(
        None,
        "--group-id",
        help="Kafka consumer group ID. Defaults to 'gcs-bronze-{entity}'.",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Loguru log level: DEBUG | INFO | SUCCESS | WARNING | ERROR",
    ),
) -> None:
    """
    Subscribe to a Kafka topic and stream messages to GCS bronze layer.

    Messages are accumulated into batches (--batch-size), serialized as NDJSON,
    then uploaded to GCS at:
        {bucket}/{entity}/year={Y}/month={M}/day={D}/batch_{id}.json

    Kafka offsets are committed ONLY after a successful GCS upload, ensuring
    exactly-once delivery semantics.

    Run one instance per topic. Use a process manager (supervisord, K8s) to
    run multiple consumers concurrently.

    Example:
        ingestion-run consumer stream \\
            --topic ecommerce.olist.orders.v1 \\
            --entity order \\
            --bucket bronze-lakehouse-my-project \\
            --batch-size 1000
    """
    from loguru import logger
    from source.services.infisical_manager import bootstrap_settings
    from source.config.settings import settings
    from source.services.storage_svc import GCSStorageService
    from source.services.kafka_consumer_svc import KafkaToGcsConsumerService

    logger.remove()
    logger.add(sys.stdout, level=log_level.upper())

    bootstrap_settings(env=env)

    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.error("KAFKA_BOOTSTRAP_SERVERS is not set.")
        raise typer.Exit(code=1)

    if not settings.GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not set.")
        raise typer.Exit(code=1)

    bucket_name = bucket or settings.GCS_BRONZE_BUCKET
    if not bucket_name:
        logger.error("GCS_BRONZE_BUCKET is not set. Provide via --bucket or .env")
        raise typer.Exit(code=1)

    storage = GCSStorageService(
        project_id=settings.GCP_PROJECT_ID,
        credentials_json=settings.GCP_SERVICE_ACCOUNT_JSON or None,
    )

    svc = KafkaToGcsConsumerService(
        kafka_bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
        topic=topic,
        entity_type=entity,
        storage=storage,
        gcs_bucket=bucket_name,
        batch_size=batch_size,
        consumer_group_id=group_id,
    )

    svc.run()


@app.command(name="stream-all")
def stream_all(
    bucket: str = typer.Option(
        None,
        "--bucket",
        "-b",
        help="GCS bucket name for bronze layer (without gs:// prefix)",
        envvar="GCS_BRONZE_BUCKET",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        help="Target environment for secret resolution: dev | staging | prod",
        envvar="APP_ENV",
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Number of messages per GCS file (applies to all topics).",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Loguru log level: DEBUG | INFO | SUCCESS | WARNING | ERROR",
    ),
) -> None:
    """
    Stream ALL registered topics to GCS concurrently (one thread per topic).

    Reads topic-entity mappings from DATASET_TOPIC_MAP and spawns one consumer
    thread per entry. Each thread logs with its entity name as context prefix,
    making it easy to distinguish log lines in aggregated output.

    NOTE: For production (Kubernetes), prefer running separate `stream` pods
    per topic — each pod has its own log stream, resource limits, and restart
    policy. Use this command for local development or staging environments only.

    Example:
        ingestion-run consumer-to-gcs stream-all --bucket bronze-lakehouse-my-project
    """
    import threading
    from loguru import logger
    from source.services.infisical_manager import bootstrap_settings
    from source.config.settings import settings
    from source.services.storage_svc import GCSStorageService
    from source.services.kafka_consumer_svc import KafkaToGcsConsumerService
    from source.mapper.configs.topic_map import DATASET_TOPIC_MAP

    logger.remove()
    logger.add(sys.stdout, level=log_level.upper())

    bootstrap_settings(env=env)

    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.error("KAFKA_BOOTSTRAP_SERVERS is not set.")
        raise typer.Exit(code=1)

    if not settings.GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID is not set.")
        raise typer.Exit(code=1)

    bucket_name = bucket or settings.GCS_BRONZE_BUCKET
    if not bucket_name:
        logger.error("GCS_BRONZE_BUCKET is not set. Provide via --bucket or .env")
        raise typer.Exit(code=1)

    # Deduplicate by topic — multiple CSV files may share the same entity/topic
    seen_topics: set[str] = set()
    consumers: list[KafkaToGcsConsumerService] = []

    for filename, config in DATASET_TOPIC_MAP.items():
        topic_value = config["topic"].value
        if topic_value in seen_topics:
            continue
        seen_topics.add(topic_value)

        # Each consumer gets its own GCSStorageService instance (thread-safety)
        storage = GCSStorageService(
            project_id=settings.GCP_PROJECT_ID,
            credentials_json=settings.GCP_SERVICE_ACCOUNT_JSON or None,
        )
        svc = KafkaToGcsConsumerService(
            kafka_bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
            topic=topic_value,
            entity_type=config["entity_type"],
            storage=storage,
            gcs_bucket=bucket_name,
            batch_size=batch_size,
        )
        consumers.append(svc)

    logger.info(f"Starting {len(consumers)} consumer thread(s)...")

    threads: list[threading.Thread] = []
    for svc in consumers:
        t = threading.Thread(
            target=svc.run,
            name=f"consumer-{svc._entity_type}",
            daemon=True,  # threads die automatically if main process exits
        )
        threads.append(t)
        t.start()
        logger.info(
            f"  ↳ Thread started | entity={svc._entity_type} | topic={svc._topic}"
        )

    logger.info("All consumer threads running. Press Ctrl+C to stop.")

    try:
        # Keep main thread alive — threads are daemon so they won't block exit
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info(
            "Shutdown signal received — waiting for threads to flush and close..."
        )
        # Each thread's run() handles its own graceful shutdown on KeyboardInterrupt


if __name__ == "__main__":
    app()
