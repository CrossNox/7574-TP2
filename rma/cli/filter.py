from typing import Dict, Union

import typer

from rma.tasks.base import Worker, VentilatorWorker
from rma.tasks.filters import (
    FilterNullUrl,
    FilterEdComment,
    FilterUniqPosts,
    FilterNanSentiment,
    FilterPostsScoreAboveMean,
)
from rma.utils import get_logger

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def uniq_posts(
    subaddr: str, reqaddr: str, pubaddr: str, repaddr: str, nsubs: int,
):
    # TODO: this is a limitation design
    # A ventilator worker should be able to declare dependencies
    worker = Worker(
        subaddr=subaddr,
        reqaddr=reqaddr,
        pubaddr=pubaddr,
        subsyncaddr=repaddr,
        nsubs=nsubs,
        executor_cls=FilterUniqPosts,
    )
    worker.run()


@app.command()
def posts_score_above_mean(
    subaddr: str,
    reqaddr: str,
    pubaddr: str,
    repaddr: str,
    nsubs: int,
    meansyncaddr: str,
    meansubaddr: str,
):
    # TODO: this is a limitation design
    # A ventilator worker should be able to declare dependencies
    worker = Worker(
        subaddr=subaddr,
        reqaddr=reqaddr,
        pubaddr=pubaddr,
        subsyncaddr=repaddr,
        nsubs=nsubs,
        executor_cls=FilterPostsScoreAboveMean,
        deps=[("mean", meansyncaddr, meansubaddr)],
    )
    worker.run()


@app.command()
def ed_comments(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    worker = VentilatorWorker(
        pulladdr=pulladdr,
        reqaddr=reqaddr,
        pushaddr=pushaddr,
        executor_cls=FilterEdComment,
    )
    worker.run()


@app.command()
def nan_sentiment(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    worker = VentilatorWorker(
        pulladdr=pulladdr,
        reqaddr=reqaddr,
        pushaddr=pushaddr,
        executor_cls=FilterNanSentiment,
    )
    worker.run()


@app.command()
def null_url(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    worker = VentilatorWorker(
        pulladdr=pulladdr,
        reqaddr=reqaddr,
        pushaddr=pushaddr,
        executor_cls=FilterNullUrl,
    )
    worker.run()
