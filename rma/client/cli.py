import csv
import json
import multiprocessing as mp
from pathlib import Path

import typer
import zmq

from rma.utils import DEFAULT_PRETTY, DEFAULT_VERBOSE, get_logger, config_logging

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(file_path: Path, addr: str):
    sent = 0
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            req.send(json.dumps(line).encode())
            req.recv()
            sent += 1

            if (sent % 10_000) == 0:
                logger.info("Sent %s rows from %s", sent, file_path)

    req.send(b"")


def get_zmqsink_memes_url(manager_list, addr):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    while True:
        req.send(b"")
        s = req.recv()

        if s == b"":
            break

        manager_list.append(json.loads(s.decode()))
    req.send(b"")
    req.recv()


def get_zmq_mean_posts_score(manager_value, addr):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)

    logger.info("Requesting mean posts score")
    req.send(b"")
    manager_value.value = float(req.recv().decode())

    logger.info("Sending ACK")
    req.send(b"")
    s = req.recv()
    logger.info("MeanPostsScore :: Got %s", s.decode())


def get_zmq_top_post(manager_value, addr):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)

    logger.info("Requesting top meme")
    req.send(b"")
    manager_value.value = req.recv()

    logger.info("Sending ACK")
    req.send(b"")
    req.recv()


@app.command()
def main(
    posts: Path = typer.Argument(..., help="Path to posts csv file"),
    comments: Path = typer.Argument(..., help="Path to comments csv file"),
    posts_relay: str = typer.Argument(..., help="Address for ZMQRelay for posts"),
    comments_relay: str = typer.Argument(..., help="Address for ZMQRelay for comments"),
    top_meme_out: Path = typer.Argument(..., help="Path to save top meme to"),
    memes_urls_sink: str = typer.Argument(
        ..., help="The address to go fetch memes results"
    ),
    mean_posts_score_sink: str = typer.Argument(
        ..., help="Address to fetch the mean posts score"
    ),
    meme_download_sink: str = typer.Argument(
        ..., help="Address to fetch top meme by sentiment"
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
    config_logging(verbose, pretty)
    logger.info("Starting processes")

    with mp.Manager() as manager:
        mean_posts_score = manager.Value(float, 0.0)
        memes_urls = manager.list()
        top_meme = manager.Value(bytes, b"")

        pposts = mp.Process(target=relay_file, args=(posts, posts_relay))
        pcomments = mp.Process(target=relay_file, args=(comments, comments_relay))

        p_mean_posts_score = mp.Process(
            target=get_zmq_mean_posts_score,
            args=(mean_posts_score, mean_posts_score_sink),
        )
        p_top_meme = mp.Process(
            target=get_zmq_top_post, args=(top_meme, meme_download_sink)
        )
        p_memes_urls = mp.Process(
            target=get_zmqsink_memes_url, args=(memes_urls, memes_urls_sink)
        )

        logger.info("Starting posts relay process")
        pposts.start()

        logger.info("Starting comments relay process")
        pcomments.start()

        logger.info("Joining posts relay process")
        pposts.join()

        logger.info("Joining comments relay process")
        pcomments.join()

        logger.info("Starting sink mean posts score process")
        p_mean_posts_score.start()

        logger.info("Starting sink top meme process")
        p_top_meme.start()

        logger.info("Starting sink memes urls process")
        p_memes_urls.start()

        logger.info("Joining mean posts score process")
        p_mean_posts_score.join()
        print(f"mean_posts_score: {mean_posts_score.value}")

        logger.info("Joining memes urls process")
        p_memes_urls.join()

        print("ed memes:")
        for i in memes_urls:
            print(f"{i}")

        logger.info("Joining top meme fetch process")
        p_top_meme.join()

        logger.info("Saving top meme to %s", top_meme_out)
        with open(top_meme_out, "wb") as f:
            f.write(top_meme.value)


if __name__ == "__main__":
    app()
