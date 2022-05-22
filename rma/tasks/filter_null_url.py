from rma.tasks.base import Filter


class FilterNullUrl(Filter):
    def filter(self, msg):
        return msg["url"] is not None
