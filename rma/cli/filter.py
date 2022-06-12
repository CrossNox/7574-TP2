import typer

from rma.utils import get_logger
from rma.tasks.base import Worker, VentilatorWorker
from rma.exceptions import SigtermError, UnsolvedDependency
from rma.tasks.filters import (
    FilterNullUrl,
    FilterEdComment,
    FilterUniqPosts,
    FilterNanSentiment,
    FilterPostsScoreAboveMean,
)

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def uniq_posts(
    subaddr: str = typer.Argument(..., help="Address to subscribe to"),
    reqaddr: str = typer.Argument(
        ..., help="Address to connect to and sync with producer"
    ),
    pubaddr: str = typer.Argument(..., help="Address to publish data to"),
    repaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with subscribers"
    ),
    nsubs: int = typer.Argument(..., help="How many subscribers are expected"),
):
    # TODO: this is a limitation design
    # A ventilator worker should be able to declare dependencies
    try:
        worker = Worker(
            subaddr=subaddr,
            reqaddr=reqaddr,
            pubaddr=pubaddr,
            subsyncaddr=repaddr,
            nsubs=nsubs,
            executor_cls=FilterUniqPosts,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def posts_score_above_mean(
    subaddr: str = typer.Argument(..., help="Address to subscribe to"),
    reqaddr: str = typer.Argument(
        ..., help="Address to connect to and sync with producer"
    ),
    pubaddr: str = typer.Argument(..., help="Address to publish data to"),
    repaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with subscribers"
    ),
    nsubs: int = typer.Argument(..., help="How many subscribers are expected"),
    meansyncaddr: str = typer.Argument(
        ..., help="Address to sync with mean calculator"
    ),
    meansubaddr: str = typer.Argument(
        ..., help="Address to subscribe to and get the mean"
    ),
):
    # TODO: this is a limitation design
    # A ventilator worker should be able to declare dependencies
    try:
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
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)
    except UnsolvedDependency:
        typer.Abort(3)


@app.command()
def ed_comments(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    try:
        worker = VentilatorWorker(
            pulladdr=pulladdr,
            reqaddr=reqaddr,
            pushaddr=pushaddr,
            executor_cls=FilterEdComment,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def nan_sentiment(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    try:
        worker = VentilatorWorker(
            pulladdr=pulladdr,
            reqaddr=reqaddr,
            pushaddr=pushaddr,
            executor_cls=FilterNanSentiment,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def null_url(
    pulladdr: str = typer.Argument(..., help="The address to pull data from"),
    reqaddr: str = typer.Argument(..., help="The address to ack the ventilator"),
    pushaddr: str = typer.Argument(..., help="The address to push data to"),
):
    try:
        worker = VentilatorWorker(
            pulladdr=pulladdr,
            reqaddr=reqaddr,
            pushaddr=pushaddr,
            executor_cls=FilterNullUrl,
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)
