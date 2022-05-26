import abc
import json
import heapq

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
        self.rep.send(b"")
        return
        # logger.info("ZMQSink :: sending poison pill")
        # logger.info("Searching for client ack")
        # self.rep.rcvtimeo = 1000
        # while True:
        #    try:
        #        self.rep.recv()
        #        self.rep.send(b"")
        #        break
        #    except zmq.ZMQError as e:
        #        if e.errno == zmq.EAGAIN:
        #            pass
        #        raise


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
                self.rep.send(b"")
                return

        logger.info("Sending meme")
        self.rep.recv()
        self.rep.send(content)

        self.rep.recv()
        self.rep.send(b"")

        # logger.info("Searching for client ack")
        # self.rep.rcvtimeo = 500
        # while True:
        #    try:
        #        self.rep.recv()
        #        self.rep.send(b"")
        #        break
        #    except zmq.ZMQError as e:
        #        if e.errno == zmq.EAGAIN:
        #            pass
        #        raise
