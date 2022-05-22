from pathlib import Path
from typing import Dict, Optional

import typer

from rma.utils import get_logger
from rma.tasks.csv_source import CSVSource

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrout": None}


@app.command()
def csv(path: Path = typer.Argument(..., help="Path to file to read")):
    csv_source = CSVSource(path, state["addrout"])
    csv_source.run()


@app.callback()
def main(
    addrout: str = typer.Argument(..., help="The address to dump into"),
):
    state["addrout"] = addrout
