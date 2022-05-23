import abc
import csv
import json

import zmq

from rma.utils import get_logger

logger = get_logger(__name__)


class Source(abc.ABC):
    def __init__(self, addrout, addrsync, nsubs: int):
        self.context = zmq.Context.instance()  # type: ignore

        self.sender = self.context.socket(zmq.PUB)
        self.sender.bind(addrout)

        self.syncservice = self.context.socket(zmq.REP)
        self.syncservice.bind(addrsync)

        self.nsubs = nsubs

    @abc.abstractmethod
    def gen(self):
        pass

    def run(self):
        subs = 0
        while subs < self.nsubs:
            _ = self.syncservice.recv()
            self.syncservice.send(b"")
            subs += 1
            logger.info(f"+1 subscriber ({subs}/{self.nsubs})")

        for thing in self.gen():
            self.sender.send(json.dumps(thing).encode())
        logger.info("Sending poison pill")
        self.sender.send(b"")


class CSVSource(Source):
    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path

    def gen(self):
        with open(self.path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row
