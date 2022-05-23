from typing import Dict

import typer

from rma.utils import get_logger
from rma.tasks.sinks import PrintSink

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, str] = {}


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
