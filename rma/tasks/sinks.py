import abc
import json
import heapq
import pathlib
from typing import Union

import zmq
import requests

from rma.utils import get_logger
from rma.constants import POISON_PILL

logger = get_logger(__name__)


class Sink(abc.ABC):
    def __init__(self, addrin, syncaddr):
        self.context = zmq.Context.instance()

        # SUB to receive data
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.connect(addrin)
        self.receiver.setsockopt_string(zmq.SUBSCRIBE, "")
        self.receiver.sndhwm = 0
        self.receiver.rcvhwm = 0

        # REQ to sync with ventilator sink
        self.syncclient = self.context.socket(zmq.REQ)
        self.syncclient.connect(syncaddr)

        self.nprocessed = 0

    @abc.abstractmethod
    def sink(self, msg):
        pass

    def final_stmt(self):
        pass

    def run(self):
        logger.debug("Sink :: syncing with source")
        self.syncclient.send(POISON_PILL)
        self.syncclient.recv()

        logger.debug("Sink :: running loop")
        while True:
            s = self.receiver.recv()
            # logger.debug("Sink :: Got message")

            if s == POISON_PILL:
                logger.debug("Sink :: got poison pill")
                break

            msg = json.loads(s.decode())
            self.sink(msg)
            self.nprocessed += 1

            if (self.nprocessed % 10_000) == 0:
                logger.debug("Sunk %s messages", self.nprocessed)

        self.final_stmt()

        self.syncclient.send(POISON_PILL)
        self.syncclient.recv()


# TODO: ZMQ Sinks should be pub/sub


class ZMQSink(Sink):
    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{port}")

    def sink(self, msg):
        self.rep.recv()
        self.rep.send(json.dumps(msg).encode())

    def final_stmt(self):
        self.rep.recv()
        self.rep.send(POISON_PILL)
        return


class PrintSink(Sink):
    def sink(self, msg):
        print(msg)


class FileSink(Sink):
    def __init__(self, path: Union[pathlib.Path, str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = pathlib.Path(path)
        self.messages = 0
        if self.path.exists():
            self.path.unlink()

    def sink(self, msg):
        logger.debug(
            "Sink :: Message number %s received. Written to %s",
            self.messages,
            self.path,
        )
        self.messages += 1
        with open(self.path, "a") as f:
            f.write(json.dumps(msg))
            f.write("\n")
            f.flush()


class TopPostDownload(Sink):
    # TODO: this could be a filter + simple download
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.top_memes = []

    def sink(self, msg):
        if msg["url"] is None or msg["url"] == "":
            return

        if "reddit.com/r" in msg["url"]:
            return

        if len(self.top_memes) >= 10:
            heapq.heappushpop(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )
        else:
            heapq.heappush(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )

    def final_stmt(self):
        logger.info("Downloading meme to %s", self.path)
        with open(self.path, "wb") as f:
            content: bytes
            while True:
                try:
                    _, _, meme = heapq.heappop(self.top_memes)
                    res = requests.get(meme["url"])
                    res.raise_for_status()
                    content = res.content
                    f.write(content)
                    break
                except requests.HTTPError:
                    pass
                except IndexError:
                    break


class TopPostZMQ(Sink):
    # TODO: this could be a filter + simple download
    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.port = port
        self.top_memes = []

        self.rep = self.context.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{port}")

    def sink(self, msg):
        if msg["url"] is None or msg["url"] == "":
            return

        if "reddit.com/r" in msg["url"]:
            return

        if len(self.top_memes) >= 10:
            heapq.heappushpop(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )
        else:
            heapq.heappush(
                self.top_memes, (-float(msg["mean_sentiment"]), self.nprocessed, msg)
            )

    def final_stmt(self):
        logger.info("Downloading the dankest meme")
        content: bytes
        while True:
            try:
                _, _, meme = heapq.heappop(self.top_memes)
                res = requests.get(meme["url"])
                res.raise_for_status()
                content = res.content
                break
            except requests.HTTPError:
                pass
            except IndexError:
                self.rep.recv()
                self.rep.send(POISON_PILL)
                return

        logger.info("Sending meme")
        self.rep.recv()
        self.rep.send(content)

        self.rep.recv()
        self.rep.send(POISON_PILL)
