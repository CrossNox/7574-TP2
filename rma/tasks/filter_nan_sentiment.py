from rma.utils import get_logger
from rma.tasks.base import Filter

logger = get_logger(__name__)


class FilterNanSentiment(Filter):
    def filter(self, msg):
        logger.info(f"{msg=}")
        return msg["sentiment"] is not None
