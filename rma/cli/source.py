from pathlib import Path
from typing import Dict, Union

import typer

from rma.tasks.sources import CSVSource, ZMQRelaySource
from rma.utils import get_logger

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int]] = {}


@app.command()
def csv(path: Path = typer.Argument(..., help="Path to file to read")):
    csv_source = CSVSource(path, **state)
    csv_source.run()


@app.command()
def zmqrelay(port: int = typer.Argument(..., help="Adress to REP port to read from")):
    zmq_relay_source = ZMQRelaySource(port=port, **state)
    zmq_relay_source.run()


@app.callback()
def main(
    addrout: str = typer.Argument(..., help="Bind address to dump into"),
    addrsync: str = typer.Argument(..., help="Bind address to sync"),
    nsubs: int = typer.Argument(..., help=""),
):
    state["addrout"] = addrout
    state["addrsync"] = addrsync
    state["nsubs"] = nsubs
