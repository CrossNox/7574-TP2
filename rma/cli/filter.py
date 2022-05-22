from typing import Dict, Optional

import typer

from rma.utils import get_logger
from rma.tasks.filter_null_url import FilterNullUrl
from rma.tasks.filter_uniq_posts import FilterUniqPosts
from rma.tasks.filter_ed_comments import FilterEdComment
from rma.tasks.filter_nan_sentiment import FilterNanSentiment
from rma.task.filter_above_score import FilterPostsScoreAboveMean

logger = get_logger(__name__)

app = typer.Typer()

state: Dict[str, Optional[str]] = {"addrin": None, "addrout": None}


@app.command()
def posts_score_above_mean():
    filter_ = FilterPostsScoreAboveMean(state["addrin"], state["addrout"])
    filter_.run()


@app.command()
def null_url():
    filter_ = FilterNullUrl(state["addrin"], state["addrout"])
    filter_.run()


@app.command()
def uniq_posts():
    filter_ = FilterUniqPosts(state["addrin"], state["addrout"])
    filter_.run()


@app.command()
def nan_sentiment():
    filter_ = FilterNanSentiment(state["addrin"], state["addrout"])
    filter_.run()


@app.command()
def ed_comments():
    filter_ed_comments = FilterEdComment(state["addrin"], state["addrout"])
    filter_ed_comments.run()


@app.callback()
def main(
    addrin: str = typer.Argument(..., help="The address to read from"),
    addrout: str = typer.Argument(..., help="The address to dump into"),
):
    state["addrin"] = addrin
    state["addrout"] = addrout
