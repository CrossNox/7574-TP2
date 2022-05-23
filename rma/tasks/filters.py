import re
import json

from rma.tasks.executor import Executor
from rma.constants import ED_KWDS_PATTERN


class FilterPostsScoreAboveMean(Executor):
    def __init__(self, mean, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mean = mean

    def handle_msg(self, msg):
        if msg["score"] >= self.mean:
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterEdComment(Executor):
    def handle_msg(self, msg):
        if re.search(ED_KWDS_PATTERN, msg["body"].lower()) is not None:
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterNanSentiment(Executor):
    def handle_msg(self, msg):
        if msg["sentiment"] is not None:
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterNullUrl(Executor):
    def handle_msg(self, msg):
        if msg["url"] is not None:
            self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class FilterUniqPosts(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_ids = set()

    def handle_msg(self, msg):
        self.post_ids.add({"post_id": msg["post_id"]})

    def final_stmt(self):
        self.task_out.send(json.dumps(list(self.post_ids)).encode())
