import csv
import json
import signal
from pathlib import Path
import multiprocessing as mp
from typing import Dict, List, Callable
from multiprocessing.managers import SyncManager
from multiprocessing.synchronize import Event as _EventClass

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


def _wrap_loop(shutdown_event: _EventClass, op: Callable, *args):
    while not shutdown_event.is_set():
        try:
            return op(*args)
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                pass
            else:
                raise


def relay_file(file_path: Path, addr: str, shutdown_event: _EventClass):
    sent = 0
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    req.RCVTIMEO = 1
    req.SNDTIMEO = 1

    try:
        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)
            for line in reader:
                if shutdown_event.is_set():
                    break
                _wrap_loop(shutdown_event, req.send, json.dumps(line).encode())
                _wrap_loop(shutdown_event, req.recv)

                sent += 1

                if (sent % 10_000) == 0:
                    logger.info("Sent %s rows from %s", sent, file_path)
            req.send(POISON_PILL)
    except zmq.error.ZMQError as e:
        if e.errno == zmq.EFSM:
            pass
        else:
            raise
    except KeyboardInterrupt:
        req.SNDTIMEO = 1
        try:
            req.send(POISON_PILL)
            req.recv()
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN or e.errno == zmq.EFSM:
                pass
            else:
                raise
        req.close()


def get_zmqsink_memes_url(manager_list, addr, shutdown_event: _EventClass):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    req.RCVTIMEO = 1
    req.SNDTIMEO = 1

    try:
        while not shutdown_event.is_set():
            _wrap_loop(shutdown_event, req.send, POISON_PILL)
            s = _wrap_loop(shutdown_event, req.recv)

            if s == POISON_PILL:
                break

            manager_list.append(json.loads(s.decode()))
    except KeyboardInterrupt:
        req.close()


def get_zmq_mean_posts_score(manager_value, addr, shutdown_event: _EventClass):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    req.RCVTIMEO = 1
    req.SNDTIMEO = 1

    logger.info("Requesting mean posts score")
    try:
        while not shutdown_event.is_set():
            _wrap_loop(shutdown_event, req.send, POISON_PILL)
            s = _wrap_loop(shutdown_event, req.recv)

            if s == POISON_PILL:
                break

            else:
                logger.info("Mean posts got %s", s.decode())
                manager_value.value = float(s)
    except KeyboardInterrupt:
        req.close()


def get_zmq_top_post(manager_value, addr, shutdown_event: _EventClass):
    ctx = zmq.Context.instance()  # type: ignore
    req = ctx.socket(zmq.REQ)
    req.connect(addr)
    req.RCVTIMEO = 1
    req.SNDTIMEO = 1

    # TODO: catch error here
    logger.info("Requesting top meme")

    try:
        _wrap_loop(shutdown_event, req.send, POISON_PILL)
        manager_value.value = _wrap_loop(shutdown_event, req.recv)
        logger.info("Sending ACK")
        _wrap_loop(shutdown_event, req.send, POISON_PILL)
        _wrap_loop(shutdown_event, req.recv)
    except KeyboardInterrupt:
        req.close()


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

    manager = SyncManager()
    manager.start(signal.signal, (signal.SIGINT | signal.SIGTERM, signal.SIG_IGN))

    with manager:
        mean_posts_score = manager.Value(float, 0.0)
        memes_urls: List[Dict[str, str]] = manager.list()
        top_meme = manager.Value(bytes, POISON_PILL)

        shutdown_event = mp.Event()

        def _shutdown():
            shutdown_event.set()

            logger.error("Joining posts relay process")
            if coalesce(pposts.is_alive)():
                pposts.join()

            logger.error("Joining comments relay process")
            if coalesce(pcomments.is_alive)():
                pposts.join()

            logger.error("Joining process to get mean posts score")
            if coalesce(p_mean_posts_score.is_alive)():
                p_mean_posts_score.join()

            logger.error("Joining process to get memes urls")
            if coalesce(p_memes_urls.is_alive)():
                p_memes_urls.join()

            logger.error("Joining process to get top meme")
            if coalesce(p_top_meme.is_alive)():
                p_top_meme.join()

        def sigterm_handler(_signum, _frame):
            logger.error("Got SIGTERM")
            _shutdown()
            exit(1)

        signal.signal(signal.SIGTERM, sigterm_handler)

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
            logger.error("Got keyboard interrupt, gracefully shutting down")
            _shutdown()


if __name__ == "__main__":
    app()
