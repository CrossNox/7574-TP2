from rma.tasks.base import Filter


class FilterPostsScoreAboveMean(Filter):
    def __init__(self, mean, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mean = mean

    def filter(self, msg):
        return msg["score"] >= self.mean
