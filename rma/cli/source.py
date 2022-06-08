from pathlib import Path
from typing import Dict, Union

import typer

from rma.utils import get_logger
from rma.exceptions import SigtermError
from rma.tasks.sources import CSVSource, ZMQRelaySource

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int]] = {}


@app.command()
def csv(path: Path = typer.Argument(..., help="Path to file to read")):
    try:
        csv_source = CSVSource(path, **state)
        csv_source.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def zmqrelay(port: int = typer.Argument(..., help="Adress to REP port to read from")):
    try:
        zmq_relay_source = ZMQRelaySource(port=port, **state)
        zmq_relay_source.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.callback()
def main(
    addrout: str = typer.Argument(..., help="Bind address to dump into"),
    addrsync: str = typer.Argument(..., help="Bind address to sync"),
    nsubs: int = typer.Argument(..., help="Amount of expected subscribers"),
):
    state["addrout"] = addrout
    state["addrsync"] = addrsync
    state["nsubs"] = nsubs
