from typing import Dict, Optional

import typer

from rma.utils import get_logger
from rma.tasks.extract_post_id import ExtractPostID

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrin": None, "addrout": None}


@app.command()
def extract_post_id():
    transformer = ExtractPostID(state["addrin"], state["addrout"])
    transformer.run()


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrout: str = typer.Argument(..., help="The address to dump into"),
):
    state["addrin"] = addrin
    state["addrout"] = addrout
