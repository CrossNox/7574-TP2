import typer

from rma.utils import get_logger
from rma.exceptions import SigtermError
from rma.tasks.base import VentilatorSink, VentilatorSource

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def source(
    subaddr: str = typer.Argument(..., help="Address where data source publishes data"),
    subsyncaddr: str = typer.Argument(..., help="Address to sync with data source"),
    pushaddr: str = typer.Argument(..., help="Address to bind and publish tasks to"),
    syncaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with workers"
    ),
    sinkaddr: str = typer.Argument(
        ..., help="The address the sink is listening on to sync"
    ),
    nworkers: int = typer.Argument(
        ..., help="How many workers will the source be splitting tasks with"
    ),
):
    try:
        worker = VentilatorSource(
            subaddr, subsyncaddr, pushaddr, syncaddr, sinkaddr, nworkers
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)


@app.command()
def sink(
    pulladdr: str = typer.Argument(..., help="Address to bind to and pull data from"),
    repaddr: str = typer.Argument(..., help="Address to bind to and sync with source"),
    pubaddr: str = typer.Argument(
        ..., help="Address to bind to and publish processed data"
    ),
    nworkers: int = typer.Argument(..., help="How many workers to pull data from"),
    subsyncaddr: str = typer.Argument(
        ..., help="Address to bind to and sync with subscribers"
    ),
    nsubs: int = typer.Argument(..., help="Amount of expected subscribers"),
):
    try:
        worker = VentilatorSink(
            pulladdr, repaddr, pubaddr, nworkers, nsubs, subsyncaddr
        )
        worker.run()
    except SigtermError:
        typer.Abort(1)
    except KeyboardInterrupt:
        typer.Abort(2)
