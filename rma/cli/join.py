from typing import Dict, List, Tuple, Union

import typer

from rma.tasks.base import Joiner
from rma.tasks.joiners import KeyJoin
from rma.utils import get_logger

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int, List[Tuple[str, str]]]] = {}


@app.command()
def bykey(
    pubaddr: str = typer.Argument(..., help="The address to publish data to"),
    repaddr: str = typer.Argument(..., help="The address to sync with subscribers"),
    subaddr: List[str] = typer.Argument(..., help="The addresses to pull data from"),
    reqaddr: List[str] = typer.Argument(
        ..., help="The addresses to ack the publishers"
    ),
    nsubs: int = typer.Argument(..., help="Number of subscribers"),
    key: str = typer.Argument(..., help="The key on which to join"),
):
    inputs = list(zip(reqaddr, subaddr))
    joiner = Joiner(
        pubaddr=pubaddr,
        subsyncaddr=repaddr,
        nsubs=nsubs,
        inputs=inputs,
        executor_cls=KeyJoin,
        executor_kwargs={"ninputs": len(state["inputs"]), "key": key},  # type: ignore
    )
    joiner.run()
