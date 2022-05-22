from collections import defaultdict

from rma.tasks.base import NonPreemptibleTransform


class MeanSentiment(NonPreemptibleTransform):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sentiment_scores = defaultdict(lambda x: {"count": 0, "sum": 0})

    def transform(self, msg):
        post_id = msg["post_id"]
        self.sentiment_scores[post_id]["count"] += 1
        self.sentiment_scores[post_id]["sum"] += msg["sentiment"]

    def agg(self):
        return {k: v["sum"] / v["count"] for k, v in self.sentiment_scores.items()}
