from typing import Dict, Optional

import typer

from rma.utils import get_logger
from rma.tasks.filter_ed_comments import FilterEdComment

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrin": None, "addrout": None}


@app.command()
def ed_comments():
    filter_ed_comments = FilterEdComment(state["addrin"], state["addrout"])
    filter_ed_comments.run()


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrout: str = typer.Argument(..., help="The address to dump into"),
):
    state["addrin"] = addrin
    state["addrout"] = addrout
