from typing import Dict, List, Tuple, Union

import typer

from rma.tasks.base import Joiner
from rma.tasks.joiners import KeyJoin
from rma.utils import get_logger

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def bykey(
    pubaddr: str = typer.Argument(..., help="The address to publish data to"),
    repaddr: str = typer.Argument(..., help="The address to sync with subscribers"),
    nsubs: int = typer.Argument(..., help="Number of subscribers"),
    subaddr: List[str] = typer.Option(..., help="The addresses to pull data from"),
    reqaddr: List[str] = typer.Option(..., help="The addresses to ack the publishers"),
    key: str = typer.Argument(..., help="The key on which to join"),
):
    inputs = list(zip(reqaddr, subaddr))
    joiner = Joiner(
        pubaddr=pubaddr,
        repaddr=repaddr,
        nsubs=nsubs,
        inputs=inputs,
        executor_cls=KeyJoin,
        executor_kwargs={"ninputs": len(inputs), "key": key},  # type: ignore
    )
    joiner.run()
