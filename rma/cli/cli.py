from pathlib import Path
from typing import Any, Dict

import typer
import yaml

from rma.cli.filter import app as filter_app
from rma.cli.join import app as join_app
from rma.cli.sink import app as sink_app
from rma.cli.source import app as source_app
from rma.cli.transform import app as transform_app
from rma.cli.ventilate import app as ventilate_app
from rma.dag.rma_dag import build_rma_dag
from rma.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(filter_app, name="filter")
app.add_typer(sink_app, name="sink")
app.add_typer(source_app, name="source")
app.add_typer(transform_app, name="transform")
app.add_typer(ventilate_app, name="ventilate")
app.add_typer(join_app, name="join")

CFG: Dict[str, Any] = {}


@app.callback()
def main(
    verbose: int = typer.Option(
        DEFAULT_VERBOSE,
        "--verbose",
        "-v",
        count=True,
        help="Level of verbosity. Can be passed more than once for more levels of logging.",
    ),
    pretty: bool = typer.Option(
        DEFAULT_PRETTY, "--pretty", help="Whether to pretty print the logs with colors"
    ),
):
    config_logging(verbose, pretty)


@app.command()
def render_dag(
    docker_compose_output: Path = typer.Argument(
        ..., help="Location where to output the docker compose file"
    ),
    dag_plot_output: Path = typer.Argument(
        ..., help="Location where to save the graphviz plot of the DAG"
    ),
    nworkers: int = typer.Argument(3, help="The amount of workers to use"),
):
    if not docker_compose_output.is_dir():
        raise ValueError("The docker compose output should be a folder")
    if not dag_plot_output.is_dir():
        raise ValueError("The dag plot output should be a folder")
    dag = build_rma_dag(nworkers=nworkers)
    with open(docker_compose_output / "docker-compose.yaml", "w") as f:
        yaml.safe_dump(dag.config, f, indent=2, width=188)

    dag.plot(dag_plot_output)


if __name__ == "__main__":
    app()
