from typing import Any, Dict

import typer

from rma.cli.sink import app as sink_app
from rma.cli.filter import app as filter_app
from rma.cli.source import app as source_app
from rma.cli.transform import app as transform_app
from rma.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(filter_app, name="filter")
app.add_typer(sink_app, name="sink")
app.add_typer(source_app, name="source")
app.add_typer(transform_app, name="transform")

CFG: Dict[str, Any] = {}


@app.callback()
def main(
    verbose: int = typer.Option(
        DEFAULT_VERBOSE,
        "--verbose",
        "-v",
        count=True,
        help="Level of verbosity. Can be passed more than once for more levels of logging.",
    ),
    pretty: bool = typer.Option(
        DEFAULT_PRETTY, "--pretty", help="Whether to pretty print the logs with colors"
    ),
):
    config_logging(verbose, pretty)


if __name__ == "__main__":
    app()