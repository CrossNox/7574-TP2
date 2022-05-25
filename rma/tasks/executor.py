import abc
import json

from rma.utils import get_logger

logger = get_logger(__name__)


class Executor(abc.ABC):
    def __init__(self, task_in, task_out):
        self.task_in = task_in
        self.task_out = task_out
        self.nprocessed = 0

    @abc.abstractmethod
    def final_stmt(self):
        pass

    @abc.abstractmethod
    def handle_msg(self, msg):
        pass

    def run(self):
        while True:
            s = self.task_in.recv()

            if s == b"":
                break

            msg = json.loads(s.decode())
            self.handle_msg(msg)
            self.nprocessed += 1
            if (self.nprocessed % 10_000) == 0:
                logger.debug("Processed %s messages", self.nprocessed)

        self.final_stmt()
