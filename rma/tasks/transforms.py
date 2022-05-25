import re
import json
from typing import List
from collections import defaultdict

from rma.utils import get_logger
from rma.tasks.executor import Executor

logger = get_logger(__name__)


class ExtractPostID(Executor):
    def handle_msg(self, msg):
        msg_id = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*", msg["permalink"]
        ).groups()
        if msg_id is None:
            logger.error("Bad permalink %s", msg["permalink"])
            return
        msg["id"] = msg_id[0]
        del msg["permalink"]
        self.task_out.send(json.dumps(msg).encode())

    def final_stmt(self):
        pass


class MeanSentiment(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentiment_scores = defaultdict(lambda: {"count": 0, "sum": 0})

    def handle_msg(self, msg):
        post_id = msg["id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += float(msg["sentiment"])

    def final_stmt(self):
        for k, v in self.sentiment_scores.items():
            msg = {"id": k, "mean_sentiment": v["sum"] / v["count"]}
            self.task_out.send(json.dumps(msg).encode())


class PostsScoreMean(Executor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.posts_scores = []

    def handle_msg(self, msg):
        self.posts_scores.append(float(msg["score"]))

    def final_stmt(self):
        retval = sum(self.posts_scores) / len(self.posts_scores)
        # TODO: send a dict
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
