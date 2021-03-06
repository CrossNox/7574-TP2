import json
from typing import Dict

from rma.utils import get_logger
from rma.constants import POISON_PILL
from rma.tasks.executor import Executor

logger = get_logger(__name__)


class KeyJoin(Executor):
    def __init__(self, ninputs: int, key: str, req_sckt, *args, **kwargs):
        if ninputs != 2:
            # Due to the fact that:
            # - We don't know the full schema
            # - Keep no record of how many updates we did on each key
            # TODO: improve this
            raise ValueError("This implementation can only handle two-way joins")
        super().__init__(*args, **kwargs)
        self.key = key
        self.inputs = ninputs
        self.joins: Dict[str, Dict[str, str]] = dict()
        self.req_sckt = req_sckt

    @classmethod
    def merge_dicts(cls, a, b):
        z = dict(a)
        z.update(b)
        return z

    def handle_msg(self, msg):

        if msg[self.key] in self.joins:
            old_dict = self.joins[msg[self.key]]
            dict_merge = KeyJoin.merge_dicts(old_dict, msg)
            self.task_out.send(json.dumps(dict_merge).encode())
            del self.joins[msg[self.key]]
        else:
            self.joins[msg[self.key]] = msg

    def final_stmt(self):
        pass

    def run(self):
        pills = 0
        while pills < self.inputs and not self.signaled_termination:
            s = self.task_in.recv()

            if s == POISON_PILL:
                logger.debug("KeyJoin :: Got a poison pill")
                # Now a good question, for when I have had more sleep
                # Do we need n pills? Or does the first one warrant we can do no more joins?
                # Does breaking here apply in all out-of-order-scenarios?
                # TODO: look this and comment the conclusion
                # Intuition: we __might__ get all A, PP(A), then B, PP(B)
                logger.debug("ACKing poison pill")
                self.req_sckt.send(POISON_PILL)
                self.req_sckt.recv()
                pills += 1
                logger.debug("Poison pill %s/%s ACKd", pills, self.inputs)
            else:
                msg = json.loads(s.decode())
                self.handle_msg(msg)

        self.final_stmt()
