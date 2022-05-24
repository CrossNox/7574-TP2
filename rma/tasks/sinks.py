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

        # REQ to sync with ventilator sink
        self.syncclient = self.context.socket(zmq.REQ)
        self.syncclient.connect(syncaddr)

    @abc.abstractmethod
    def sink(self, msg):
        pass

    def final_stmt(self):
        pass

    def run(self):
        logger.info("Sink :: syncing with source")
        self.syncclient.send(b"")
        self.syncclient.recv()

        logger.info("Sink :: running loop")
        while True:
            s = self.receiver.recv()
            logger.info("Sink :: Got message")

            if s == b"":
                logger.info("Sink :: got poison pill")
                break

            msg = json.loads(s.decode())
            self.sink(msg)

        self.final_stmt()


class PrintSink(Sink):
    def sink(self, msg):
        print(msg)


class FileSink(Sink):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.messages = 0

    def sink(self, msg):
        logger.info("Sink :: Message number %s received", self.messages)
        self.messages += 1
        with open(self.path, "a") as f:
            f.write(json.dumps(msg))
            f.write("\n")
            f.flush()


class TopPostDownload(Sink):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.top_meme = None

    def sink(self, msg):
        if msg["url"] is None:
            return

        if self.top_meme is None or float(self.top_meme["mean_sentiment"]) >= float(
            msg["mean_sentiment"]
        ):
            self.top_meme = msg

    def final_stmt(self):
        with open(self.path, "wb") as f:
            res = requests.get(self.top_meme["url"])
            res.raise_for_status()
            f.write(res.content)
