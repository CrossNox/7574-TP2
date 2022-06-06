import csv
import json
from pathlib import Path
import multiprocessing as mp
from typing import Dict, List, Callable

import zmq
import typer

from rma.constants import POISON_PILL
from rma.utils import (
    DEFAULT_PRETTY,
    DEFAULT_VERBOSE,
    coalesce,
    get_logger,
    config_logging,
)

logger = get_logger(__name__)

app = typer.Typer()


def relay_file(file_path: Path, addr: str, shutdown_event: mp.synchronize.Event):
    sent = 0
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for line in reader:
            if shutdown_event.is_set():
                break
            req.send(json.dumps(line).encode())
            req.recv()
            sent += 1

            if (sent % 10_000) == 0:
                logger.info("Sent %s rows from %s", sent, file_path)

    req.send(POISON_PILL)


def get_zmqsink_memes_url(manager_list, addr, shutdown_event: mp.synchronize.Event):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)

    while not shutdown_event.is_set():
        req.send(POISON_PILL)
        s = req.recv()

        if s == POISON_PILL:
            break

        manager_list.append(json.loads(s.decode()))


def get_zmq_mean_posts_score(manager_value, addr, shutdown_event: mp.synchronize.Event):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)

    logger.info("Requesting mean posts score")
    while not shutdown_event.is_set():
        req.send(POISON_PILL)
        s = req.recv()

        if s == POISON_PILL:
            break

        else:
            logger.info("Mean posts got %s", s.decode())
            manager_value.value = float(s)


def get_zmq_top_post(manager_value, addr, shutdown_event: mp.synchronize.Event):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    req.RCVTIMEO = 1
    req.SNDTIMEO = 1

    # TODO: catch error here
    logger.info("Requesting top meme")

    def _wrap_loop(op: Callable, *args):
        while not shutdown_event.is_set():
            try:
                return op(*args)
            except zmq.error.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

    _wrap_loop(req.send, POISON_PILL)
    manager_value.value = _wrap_loop(req.recv)
    logger.info("Sending ACK")
    _wrap_loop(req.send, POISON_PILL)
    _wrap_loop(req.recv)


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
        memes_urls: List[Dict[str, str]] = manager.list()
        top_meme = manager.Value(bytes, POISON_PILL)

        shutdown_event = mp.Event()

        try:
            pposts = mp.Process(
                target=relay_file, args=(posts, posts_relay, shutdown_event)
            )
            pcomments = mp.Process(
                target=relay_file, args=(comments, comments_relay, shutdown_event)
            )

            p_mean_posts_score = mp.Process(
                target=get_zmq_mean_posts_score,
                args=(mean_posts_score, mean_posts_score_sink, shutdown_event),
            )
            p_top_meme = mp.Process(
                target=get_zmq_top_post,
                args=(top_meme, meme_download_sink, shutdown_event),
            )
            p_memes_urls = mp.Process(
                target=get_zmqsink_memes_url,
                args=(memes_urls, memes_urls_sink, shutdown_event),
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

        except KeyboardInterrupt:
            logger.info("Got keyboard interrupt, gracefully shutting down")
            shutdown_event.set()
            coalesce(pposts.join)()
            coalesce(pcomments.join)()
            coalesce(p_mean_posts_score.join)()
            coalesce(p_memes_urls.join)()
            coalesce(p_top_meme.join)()


if __name__ == "__main__":
    app()
