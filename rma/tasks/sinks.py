import abc
import json

import zmq

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

    def run(self):
        self.syncclient.send(b"")
        self.syncclient.recv()

        while True:
            s = self.receiver.recv()

            if s == b"":
                break

            msg = json.loads(s.decode())
            self.sink(msg)


class PrintSink(Sink):
    def sink(self, msg):
        print(msg)
