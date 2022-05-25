import csv
import json
import multiprocessing as mp
from pathlib import Path

import typer
import zmq

from rma.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(file_path: Path, addr: str):
    ctx = zmq.Context.instance()
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            req.send(json.dumps(line).encode())
            req.recv()
    req.send(b"")


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    posts_relay: str = typer.Argument(..., help="Address for ZMQRelay for posts"),
    comments_relay: str = typer.Argument(..., help="Address for ZMQRelay for comments"),
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
    logger.info("Starting processes")
    pposts = mp.Process(target=relay_file, args=(posts, posts_relay))
    pcomments = mp.Process(target=relay_file, args=(comments, comments_relay))
    pposts.start()
    pcomments.start()
    pposts.join()
    pcomments.join()


if __name__ == "__main__":
    app()
