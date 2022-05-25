from typing import List

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


@app.command()
def posts_score_mean(
    subaddr: str,
    reqaddr: str,
    pubaddr: str,
    repaddr: str,
    nsubs: int,
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
    nsubs: int,
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
def filter_columns(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
    columns: List[str] = typer.Argument(..., help="Columns to keep"),
):
    worker = VentilatorWorker(
        pulladdr=pulladdr,
        reqaddr=reqaddr,
        pushaddr=pushaddr,
        executor_cls=FilterColumn,
        executor_kwargs={"columns": columns},
    )
    worker.run()


@app.command()
def extract_post_id(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    worker = VentilatorWorker(
        pulladdr=pulladdr,
        reqaddr=reqaddr,
        pushaddr=pushaddr,
        executor_cls=ExtractPostID,
    )
    worker.run()
