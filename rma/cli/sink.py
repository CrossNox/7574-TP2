from typing import Dict, Optional

import typer

from rma.utils import get_logger
from rma.tasks.print_sink import PrintSink

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrin": None}


@app.command()
def printmsg():
    print_sink = PrintSink(state["addrin"])
    print_sink.run()


@app.callback()
def main(addrin: str = typer.Argument(..., help="The address to read from")):
    state["addrin"] = addrin
