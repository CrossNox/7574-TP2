from rma.tasks.base import Filter


class FilterNanSentiment(Filter):
    def filter(self, msg):
        return msg["sentiment"] is not None
