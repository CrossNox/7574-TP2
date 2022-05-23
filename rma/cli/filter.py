from typing import Dict, Union

import typer

from rma.utils import get_logger
from rma.tasks.base import VentilatorWorker
from rma.tasks.filters import FilterEdComment

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int]] = {}


@app.command()
def ed_comments():
    worker = VentilatorWorker(**state, executor_cls=FilterEdComment)
    worker.run()


@app.callback()
def main(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    state["pulladdr"] = pulladdr
    state["reqaddr"] = reqaddr
    state["pushaddr"] = pushaddr
