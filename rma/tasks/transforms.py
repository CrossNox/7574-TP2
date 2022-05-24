import re
import json
from typing import List
from collections import defaultdict

from rma.utils import get_logger
from rma.tasks.executor import Executor

logger = get_logger(__name__)


class ExtractPostID(Executor):
    def handle_msg(self, msg):
        msg["id"] = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*", msg["permalink"]
        ).groups()[0]
        del msg["permalink"]
        self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class MeanSentiment(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})

    def handle_msg(self, msg):
        post_id = msg["post_id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += msg["sentiment"]

    def final_stmt(self):
        for k, v in self.sentiment_scores.items():
            msg = {"post_id": k, "mean_sentiment": v["sum"] / v["count"]}
            self.task_out.send(json.dumps(msg).encode())


class PostsScoreMean(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.posts_scores = []

    def handle_msg(self, msg):
        self.posts_scores.append(float(msg["score"]))

    def final_stmt(self):
        retval = sum(self.posts_scores) / len(self.posts_scores)
        self.task_out.send(json.dumps(retval).encode())


class FilterColumn(Executor):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def handle_msg(self, msg):
        msg = {k: v for k, v in msg.items() if k in self.columns}
        self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass
