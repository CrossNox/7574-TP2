import os
import pathlib
from contextlib import contextmanager

import typer
import pandas as pd

from rma.cfg import cfg
from rma.utils import (
    DEFAULT_PRETTY,
    DEFAULT_VERBOSE,
    get_logger,
    path_or_none,
    config_logging,
)

logger = get_logger(__name__)

app = typer.Typer()

KAGGLE_FOLDER = pathlib.Path.home() / ".kaggle"
DATA_FOLDER = pathlib.Path(__name__).parent.parent / "notebooks" / "data"
POSTS_FILENAME = "the-reddit-irl-dataset-posts.csv"
COMMENTS_FILENAME = "the-reddit-irl-dataset-comments.csv"


@contextmanager
def environment_variable(key, value):
    prev_value = os.getenv(key)
    os.environ[key] = str(value)
    try:
        yield
    finally:
        if prev_value is not None:
            os.environ[key] = prev_value
        else:
            del os.environ[key]


@app.command()
def main(
    kaggle_json_loc: pathlib.Path = typer.Argument(
        cfg.kaggle.json_loc(default=KAGGLE_FOLDER, cast=path_or_none),
        help="Path to the folder containing the json with the Kaggle credentials",
    ),
    outputdir: pathlib.Path = typer.Argument(
        cfg.data.outputdir(default=DATA_FOLDER, cast=path_or_none),
        help="Path where to save data to",
    ),
    sample_size: float = typer.Option(
        0.01, help="Sample size for reduced datasets", min=0.001, max=1.0
    ),
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
    if not outputdir.is_dir():
        raise ValueError("The output dir should be a directory, not a file")
    if not kaggle_json_loc.is_dir():
        raise ValueError(
            "The kaggle json location should be a directory with a kaggle.json file in it, not a file"
        )

    config_logging(verbose, pretty)
    with environment_variable("KAGGLE_CONFIG_DIR", kaggle_json_loc):
        logger.info("Authenticating kaggle API with file on %s", kaggle_json_loc)
        from kaggle import KaggleApi

        api = KaggleApi()

        if (
            not pathlib.Path(outputdir / COMMENTS_FILENAME).exists()
            or not pathlib.Path(outputdir / POSTS_FILENAME).exists()
        ):
            logger.info("Downloading data to %s", outputdir)
            api.dataset_download_files(
                "pavellexyr/the-reddit-irl-dataset",
                path=outputdir,
                quiet=False,
                unzip=True,
            )
        else:
            logger.info("Data already downloaded")

        reduced_posts_out = (
            outputdir / f"{pathlib.Path(outputdir / POSTS_FILENAME).stem}-reduced.csv"
        )
        logger.info("Saving reduced posts dataset to %s", reduced_posts_out)
        df_posts = pd.read_csv(outputdir / POSTS_FILENAME)
        n_posts = int(len(df_posts) * sample_size)
        df_posts.head(n_posts).to_csv(reduced_posts_out, index=False)

        reduced_comments_out = (
            outputdir
            / f"{pathlib.Path(outputdir / COMMENTS_FILENAME).stem}-reduced.csv"
        )
        logger.info("Saving reduced comments dataset to %s", reduced_comments_out)
        df_comments = pd.read_csv(outputdir / COMMENTS_FILENAME)
        n_comments = int(len(df_comments) * sample_size)
        df_comments.head(n_comments).to_csv(
            reduced_comments_out,
            index=False,
        )

        logger.info("All data downloaded")


if __name__ == "__main__":
    app()
