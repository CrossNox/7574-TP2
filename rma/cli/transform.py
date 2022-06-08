from typing import List

import typer

from rma.utils import get_logger
from rma.exceptions import SigtermError
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
    subaddr: str = typer.Argument(
        ..., help="Address to connect to and fetch data from"
    ),
    reqaddr: str = typer.Argument(
        ..., help="Address to connect to and sync with producer"
    ),
    pubaddr: str = typer.Argument(..., help="Address to bind to and publish results"),
    repaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with subscribers"
    ),
    nsubs: int = typer.Argument(..., help="Amount of expected subscribers"),
):
    try:
        worker = Worker(
            subaddr=subaddr,
            reqaddr=reqaddr,
            pubaddr=pubaddr,
            subsyncaddr=repaddr,
            nsubs=nsubs,
            executor_cls=PostsScoreMean,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def mean_sentiment(
    subaddr: str = typer.Argument(
        ..., help="Address to connect to and fetch data from"
    ),
    reqaddr: str = typer.Argument(
        ..., help="Address to connect to and sync with producer"
    ),
    pubaddr: str = typer.Argument(..., help="Address to bind to and publish results"),
    repaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with subscribers"
    ),
    nsubs: int = typer.Argument(..., help="Amount of expected subscribers"),
):
    try:
        worker = Worker(
            subaddr=subaddr,
            reqaddr=reqaddr,
            pubaddr=pubaddr,
            subsyncaddr=repaddr,
            nsubs=nsubs,
            executor_cls=MeanSentiment,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def filter_columns(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
    columns: List[str] = typer.Argument(..., help="Columns to keep"),
):
    try:
        worker = VentilatorWorker(
            pulladdr=pulladdr,
            reqaddr=reqaddr,
            pushaddr=pushaddr,
            executor_cls=FilterColumn,
            executor_kwargs={"columns": columns},
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def extract_post_id(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    try:
        worker = VentilatorWorker(
            pulladdr=pulladdr,
            reqaddr=reqaddr,
            pushaddr=pushaddr,
            executor_cls=ExtractPostID,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)
