import abc
import json

import zmq
import requests

from rma.utils import get_logger

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
        self.syncclient.send(b"")
        self.syncclient.recv()

        logger.debug("Sink :: running loop")
        while True:
            s = self.receiver.recv()
            # logger.debug("Sink :: Got message")

            if s == b"":
                logger.debug("Sink :: got poison pill")
                break

            msg = json.loads(s.decode())
            self.sink(msg)
            self.nprocessed += 1

            if (self.nprocessed % 10_000) == 0:
                logger.debug("Sunk %s messages", self.nprocessed)

        self.final_stmt()

        self.syncclient.send(b"")
        self.syncclient.recv()


class PrintSink(Sink):
    def sink(self, msg):
        print(msg)


class FileSink(Sink):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
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
        self.top_meme = None

    def sink(self, msg):
        if msg["url"] is None or msg["url"] == "":
            return

        if "reddit.com/r" in msg["url"]:
            return

        if self.top_meme is None or float(self.top_meme["mean_sentiment"]) <= float(
            msg["mean_sentiment"]
        ):
            self.top_meme = msg

    def final_stmt(self):
        logger.info("Downloading meme %s to %s", self.top_meme, self.path)
        with open(self.path, "wb") as f:
            res = requests.get(self.top_meme["url"])
            res.raise_for_status()
            f.write(res.content)
