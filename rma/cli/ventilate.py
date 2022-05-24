import typer

from rma.utils import get_logger
from rma.tasks.base import VentilatorSink, VentilatorSource

logger = get_logger(__name__)

app = typer.Typer()


@app.command()
def source(
    subaddr: str = typer.Argument(..., help="Conn"),
    subsyncaddr: str = typer.Argument(..., help="Conn"),
    pushaddr: str = typer.Argument(..., help="Bind"),
    syncaddr: str = typer.Argument(..., help="Bind"),
    sinkaddr: str = typer.Argument(..., help="Conn"),
    nworkers: int = typer.Argument(..., help=""),
):
    worker = VentilatorSource(
        subaddr, subsyncaddr, pushaddr, syncaddr, sinkaddr, nworkers
    )
    worker.run()


@app.command()
def sink(
    pulladdr: str = typer.Argument(..., help="Bind"),
    repaddr: str = typer.Argument(..., help="Bind"),
    pubaddr: str = typer.Argument(..., help="Bind"),
    nworkers: int = typer.Argument(..., help=""),
    subsyncaddr: str = typer.Argument(..., help="Bind"),
    nsubs: int = typer.Argument(..., help=""),
):
    worker = VentilatorSink(pulladdr, repaddr, pubaddr, nworkers, nsubs, subsyncaddr)
    worker.run()
