from typing import Dict, List, Union

import typer

from rma.utils import get_logger
from rma.tasks.base import Worker, VentilatorWorker
from rma.tasks.transforms import (
    FilterColumn,
    ExtractPostID,
    MeanSentiment,
    PostsScoreMean,
)

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Union[str, int]] = {}


@app.command()
def posts_score_mean(
    subaddr: str,
    reqaddr: str,
    pubaddr: str,
    repaddr: str,
    nsubs: int = 1,
):
    worker = Worker(
        subaddr=subaddr,
        reqaddr=reqaddr,
        pubaddr=pubaddr,
        subsyncaddr=repaddr,
        nsubs=nsubs,
        executor_cls=PostsScoreMean,
    )
    worker.run()


@app.command()
def mean_sentiment(
    subaddr: str,
    reqaddr: str,
    pubaddr: str,
    repaddr: str,
    nsubs: int = 1,
):
    worker = Worker(
        subaddr=subaddr,
        reqaddr=reqaddr,
        pubaddr=pubaddr,
        subsyncaddr=repaddr,
        nsubs=nsubs,
        executor_cls=MeanSentiment,
    )
    worker.run()


@app.command()
def filter_columns(columns: List[str]):
    worker = VentilatorWorker(
        **state, executor_cls=FilterColumn, executor_kwargs={"columns": columns}
    )
    worker.run()


@app.command()
def extract_post_id():
    worker = VentilatorWorker(**state, executor_cls=ExtractPostID)
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
