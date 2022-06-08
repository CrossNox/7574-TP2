from typing import Dict
from pathlib import Path

import typer

from rma.utils import get_logger
from rma.exceptions import SigtermError
from rma.tasks.sinks import ZMQSink, FileSink, PrintSink, TopPostZMQ, TopPostDownload

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, str] = {}


@app.command()
def top_post(path: Path = typer.Argument(..., help="Path to download the top post to")):
    try:
        top_post_sink = TopPostDownload(path=path, **state)
        top_post_sink.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def zmq_top_post(
    port: int = typer.Argument(
        ..., help="Port to bind to and listen for incoming requests for the best meme"
    )
):
    try:
        top_post_sink = TopPostZMQ(port=port, **state)
        top_post_sink.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def tofile(path: Path = typer.Argument(..., help="Path to write messages to")):
    try:
        file_sink = FileSink(path=path, **state)
        file_sink.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def printmsg():
    try:
        print_sink = PrintSink(**state)
        print_sink.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def zmqsink(
    port: int = typer.Argument(
        ..., help="Port to bind to and reply to incoming requests for messages"
    )
):
    try:
        zmq_sink = ZMQSink(port=port, **state)
        zmq_sink.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrsync: str = typer.Argument(..., help="Address to sync with"),
):
    state["addrin"] = addrin
    state["syncaddr"] = addrsync
