from typing import Optional

import typer
from source.controller import producer
from source.controller import consumer_to_gcs

import importlib.metadata

# === INFORMATION ===
try:
    __app_name__ = importlib.metadata.metadata("lakehouse-ingestion-engine").get(
        "Name", "Lakehouse Ingestion Engine"
    )
    __version__ = importlib.metadata.version("lakehouse-ingestion-engine")
except importlib.metadata.PackageNotFoundError:
    __app_name__ = "Lakehouse Ingestion Engine (Dev)"
    __version__ = "0.0.0-dev"

app = typer.Typer()

app.add_typer(producer.app, name="producer")
app.add_typer(consumer_to_gcs.app, name="consumer-to-gcs")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return


if __name__ == "__main__":
    app()
