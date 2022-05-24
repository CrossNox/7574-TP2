from typing import Dict, List, Tuple, Union

import typer

from rma.utils import get_logger
from rma.tasks.base import Joiner
from rma.tasks.joiners import KeyJoin

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int, List[Tuple[str, str]]]] = {}


@app.command()
def bykey(key: str = typer.Argument(..., help="The key on which to join")):
    joiner = Joiner(
        **state,
        executor_cls=KeyJoin,
        executor_kwargs={"ninputs": len(state["inputs"]), "key": key}  # type: ignore
    )
    joiner.run()


@app.callback()
def main(
    pubaddr: str = typer.Argument(..., help="The address to publish data to"),
    repaddr: str = typer.Argument(..., help="The address to sync with subscribers"),
    subaddr: List[str] = typer.Argument(..., help="The addresses to pull data from"),
    reqaddr: List[str] = typer.Argument(
        ..., help="The addresses to ack the publishers"
    ),
    nsubs: int = typer.Argument(..., help="Number of subscribers"),
):
    state["pubaddr"] = pubaddr
    state["subsyncaddr"] = repaddr
    state["inputs"] = list(zip(reqaddr, subaddr))
    state["nsubs"] = nsubs
