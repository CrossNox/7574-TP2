from typing import Any, Dict

import typer

from rma.cfg import cfg
from rma.constants import DEFAULT_HOST, DEFAULT_PORT
from rma.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()
CFG: Dict[str, Any] = {}


@app.command()
def cmd1():
    """CMD 1"""
    host = CFG["host"]
    port = CFG["port"]
    logger.info("CMD1 %s %s", host, port)


@app.callback()
def main(
    host: str = typer.Option(
        cfg.server.host(default=DEFAULT_HOST), help="Host address of the server"
    ),
    port: int = typer.Option(
        cfg.server.port(default=DEFAULT_PORT, cast=int), help="Port of the server"
    ),
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
    CFG["host"] = host
    CFG["port"] = port


if __name__ == "__main__":
    app()
