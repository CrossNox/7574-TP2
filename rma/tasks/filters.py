import json
import re

from rma.constants import ED_KWDS_PATTERN
from rma.tasks.executor import Executor
from rma.utils import get_logger

logger = get_logger(__name__)


class FilterPostsScoreAboveMean(Executor):
    def __init__(self, mean, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mean = float(mean)

    def handle_msg(self, msg):
        if int(msg["score"]) >= self.mean:
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterEdComment(Executor):
    def handle_msg(self, msg):
        if re.search(ED_KWDS_PATTERN, msg["body"].lower()) is not None:
            del msg["body"]  # TODO: remove from here
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterNanSentiment(Executor):
    def handle_msg(self, msg):
        try:
            float(msg["sentiment"])
            self.task_out.send(json.dumps(msg).encode())
        except:  # pylint: disable=bare-except
            pass

    def final_stmt(self):
        pass


class FilterNullUrl(Executor):
    def handle_msg(self, msg):
        if msg["url"] is not None and msg["url"] != "":
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterUniqPosts(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_ids = set()

    def handle_msg(self, msg):
        self.post_ids.add(msg["id"])

    def final_stmt(self):
        for post_id in self.post_ids:
            msg = {"id": post_id}
            self.task_out.send(json.dumps(msg).encode())
