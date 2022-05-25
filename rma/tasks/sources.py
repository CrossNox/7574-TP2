import abc
import csv
import json

import zmq

from rma.utils import get_logger

logger = get_logger(__name__)


class Source(abc.ABC):
    def __init__(self, addrout, addrsync, nsubs: int):
        self.context = zmq.Context.instance()  # type: ignore

        # PUB to publish data
        self.sender = self.context.socket(zmq.PUB)
        self.sender.sndhwm = 1100000
        self.sender.bind(addrout)

        # REP to sync with all subscribers
        self.syncservice = self.context.socket(zmq.REP)
        self.syncservice.bind(addrsync)

        self.nsubs = nsubs

        logger.debug("Source :: dumping into %s", addrout)
        logger.debug("Source :: sync addr %s", addrsync)

    @abc.abstractmethod
    def gen(self):
        pass

    def run(self):
        logger.debug("Source :: syncing with subs")
        subs = 0
        while subs < self.nsubs:
            _ = self.syncservice.recv()
            self.syncservice.send(b"")
            subs += 1
            logger.debug(f"Source :: +1 subscriber ({subs}/{self.nsubs})")

        logger.debug("Source: generating")
        for thing in self.gen():
            self.sender.send(json.dumps(thing).encode())

        logger.debug("Source :: Sending poison pill")
        self.sender.send(b"")


class CSVSource(Source):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.ngen = 0

    def gen(self):
        with open(self.path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.ngen += 1
                if (self.ngen % 100_000) == 0:
                    logger.info("%s messages sent", self.ngen)
                yield row
