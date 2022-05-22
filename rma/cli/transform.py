from typing import Dict, List, Optional

import typer

from rma.utils import get_logger
from rma.tasks.score_mean import PostsScoreMean
from rma.tasks.filter_columns import FilterColumns
from rma.tasks.mean_sentiment import MeanSentiment
from rma.tasks.extract_post_id import ExtractPostID

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrin": None, "addrout": None}


@app.command()
def score_mean(cols: List[str]):
    transformer = PostsScoreMean(cols, state["addrin"], state["addrout"])
    transformer.run()


@app.command()
def filter_columns(cols: List[str]):
    transformer = FilterColumns(cols, state["addrin"], state["addrout"])
    transformer.run()


@app.command()
def mean_sentiment():
    transformer = MeanSentiment(state["addrin"], state["addrout"])
    transformer.run()


@app.command()
def extract_post_id():
    transformer = ExtractPostID(state["addrin"], state["addrout"])
    transformer.run()


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrout: str = typer.Argument(..., help="The address to dump into"),
):
    state["addrin"] = addrin
    state["addrout"] = addrout
