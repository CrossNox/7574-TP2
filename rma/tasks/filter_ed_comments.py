import re

from rma.utils import get_logger
from rma.tasks.base import Filter
from rma.constants import ED_KWDS_PATTERN

logger = get_logger(__name__)


class FilterEdComment(Filter):
    def filter(self, msg):
        return re.search(ED_KWDS_PATTERN, msg["body"].lower()) is not None
