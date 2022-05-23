import abc
import json


class Executor(abc.ABC):
    def __init__(self, task_in, task_out):
        self.task_in = task_in
        self.task_out = task_out

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

        self.final_stmt()
        self.task_out.send(b"")
