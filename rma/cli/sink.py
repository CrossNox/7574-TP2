from typing import Dict
from pathlib import Path

import typer

from rma.utils import get_logger
from rma.tasks.sinks import FileSink, PrintSink, TopPostDownload

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, str] = {}


@app.command()
def top_post(path: Path):
    top_post_sink = TopPostDownload(
        path=path, addrin=state["addrin"], syncaddr=state["addrsync"]
    )
    top_post_sink.run()


@app.command()
def tofile(path: Path):
    file_sink = FileSink(path=path, addrin=state["addrin"], syncaddr=state["addrsync"])
    file_sink.run()


@app.command()
def printmsg():
    print_sink = PrintSink(addrin=state["addrin"], syncaddr=state["addrsync"])
    print_sink.run()


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrsync: str = typer.Argument(..., help=""),
):
    state["addrin"] = addrin
    state["addrsync"] = addrsync
