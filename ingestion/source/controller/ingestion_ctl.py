import time
import typer
import sys
import json
import jmespath
from loguru import logger

app = typer.Typer()


@app.command()
def engine(
    mode: str = typer.Option(
        "mode for running the engine [DEBUG|INFO|SUCCES|WARNING|ERROR]"
    ),
):
    logger.remove()
    logger.add(sys.stdout, level=mode)
    Engine().consume()


if __name__ == "__main__":
    app()


class Engine:
    def __init__(self, *args, **kwargs) -> None:
        # self.run_smoke_test()
        self.init_var()
