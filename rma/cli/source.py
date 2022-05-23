from pathlib import Path
from typing import Dict, Union

import typer

from rma.utils import get_logger
from rma.tasks.csv_source import CSVSource

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int]] = {}


@app.command()
def csv(path: Path = typer.Argument(..., help="Path to file to read")):
    csv_source = CSVSource(
        path, addrout=state["addrout"], addrsync=state["addrsync"], nsubs=state["nsubs"]
    )
    csv_source.run()


@app.callback()
def main(
    addrout: str = typer.Argument(..., help="The address to dump into"),
    syncaddr: str = typer.Argument(..., help="The address to sync"),
    nsubs: int = typer.Argument(..., help=""),
):
    state["addrout"] = addrout
    state["addrsync"] = syncaddr
    state["nsubs"] = nsubs
