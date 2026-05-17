"""
controller/producer.py — Typer CLI Entry Point for the Kafka Producer

Responsibility:
    Parse CLI arguments and delegate execution to CsvKafkaProducerService.
    This file contains ZERO business logic — it is a pure CLI adapter.

Commands:
    engine       Run the full ingestion pipeline (all mapped CSV files).
    single-file  Run ingestion for a single named CSV file.

Usage:
    python -m source.main producer engine --data-dir /data/olist --env dev
    python -m source.main producer single-file --filename olist_orders_dataset.csv
"""

import sys
import typer

app = typer.Typer(help="Kafka Producer — CSV → EventEnvelope → Kafka")


@app.command()
def engine(
    data_dir: str = typer.Option(
        ...,
        "--data-dir",
        "-d",
        help="Absolute or relative path to the directory containing Olist CSV files.",
        envvar="PRODUCER_DATA_DIR",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        "-e",
        help="Target ingestion environment: dev | staging | prod",
        envvar="APP_ENV",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Loguru log level: DEBUG | INFO | SUCCESS | WARNING | ERROR",
    ),
    pipeline_run_id: str = typer.Option(
        None,
        "--run-id",
        help="Optional pipeline run ID for lineage tracking. Auto-generated if not provided.",
    ),
) -> None:
    """
    Run the full CSV → Kafka ingestion pipeline.

    Scans --data-dir for all CSV files registered in DATASET_TOPIC_MAP
    and produces each row as an EventEnvelope to the correct Kafka topic.
    """
    from pathlib import Path
    from loguru import logger
    from source.config.settings import settings
    from source.services.kafka_producer_svc import CsvKafkaProducerService

    logger.remove()
    logger.add(sys.stdout, level=log_level.upper())

    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.error(
            "KAFKA_BOOTSTRAP_SERVERS is not set. "
            "Configure it via .env or Infisical before running."
        )
        raise typer.Exit(code=1)

    svc = CsvKafkaProducerService(
        kafka_bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
        data_dir=Path(data_dir),
        ingestion_env=env,
        pipeline_run_id=pipeline_run_id,
    )
    svc.run()


@app.command(name="single-file")
def single_file(
    filename: str = typer.Option(
        ...,
        "--filename",
        "-f",
        help="Basename of the CSV file to ingest, e.g. 'olist_orders_dataset.csv'",
    ),
    data_dir: str = typer.Option(
        ...,
        "--data-dir",
        "-d",
        help="Path to the directory containing the CSV file.",
        envvar="PRODUCER_DATA_DIR",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        "-e",
        help="Target ingestion environment: dev | staging | prod",
        envvar="APP_ENV",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Loguru log level: DEBUG | INFO | SUCCESS | WARNING | ERROR",
    ),
) -> None:
    """
    Produce a single named CSV file to Kafka (useful for targeted re-ingestion).

    Example:
        python -m source.main producer single-file \\
            --filename olist_orders_dataset.csv \\
            --data-dir /data/olist
    """
    from pathlib import Path
    from loguru import logger
    from source.config.settings import settings
    from source.services.kafka_producer_svc import CsvKafkaProducerService

    logger.remove()
    logger.add(sys.stdout, level=log_level.upper())

    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.error("KAFKA_BOOTSTRAP_SERVERS is not set.")
        raise typer.Exit(code=1)

    svc = CsvKafkaProducerService(
        kafka_bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
        data_dir=Path(data_dir),
        ingestion_env=env,
    )
    svc.run_single_file(filename=filename)


if __name__ == "__main__":
    app()
