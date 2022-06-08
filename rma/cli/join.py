from typing import List

import typer

from rma.utils import get_logger
from rma.tasks.base import Joiner
from rma.tasks.joiners import KeyJoin
from rma.exceptions import SigtermError

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
    try:
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
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)
