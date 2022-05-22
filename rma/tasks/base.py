import abc
import json

import zmq

from rma.utils import get_logger

logger = get_logger(__name__)


class Task(abc.ABC):
    @abc.abstractmethod
    def run(self):
        pass


class Source(Task, abc.ABC):
    def __init__(self, addrout, context=None):
        logger.info(f"{addrout=}")
        self.context = context or zmq.Context()

        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind(addrout)

    @abc.abstractmethod
    def gen(self):
        pass

    def run(self):
        for thing in self.gen():
            self.sender.send(json.dumps(thing).encode())
        logger.info("Sending poison pill")
        self.sender.send(b"")


class Sink(Task, abc.ABC):
    def __init__(self, addrin, context=None):
        self.context = context or zmq.Context()

        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(addrin)

    @abc.abstractmethod
    def sink(self, msg):
        pass

    def run(self):
        while True:
            s = self.receiver.recv()

            if s == b"":
                break

            msg = json.loads(s.decode())
            self.sink(msg)


class Step(Task, abc.ABC):
    def __init__(self, addrin, addrout, context=None):
        self.context = context or zmq.Context()

        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(addrin)

        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(addrout)

    @abc.abstractmethod
    def exec_step(self, msg):
        pass

    def run(self):
        while True:
            s = self.receiver.recv()

            if s == b"":
                break

            msg = json.loads(s.decode())
            self.exec_step(msg)
        self.sender.send(b"")


class Transform(Step, abc.ABC):
    @abc.abstractmethod
    def transform(self, msg):
        pass

    def exec_step(self, msg):
        msg = self.transform(msg)
        self.sender.send(json.dumps(msg).encode())


class Filter(Step, abc.ABC):
    @abc.abstractmethod
    def filter(self, msg):
        pass

    def exec_step(self, msg):
        if self.filter(msg):
            self.sender.send(json.dumps(msg).encode())
