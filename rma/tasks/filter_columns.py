from typing import List

from rma.tasks.base import Transform


class FilterColumn(Transform):
    def __init__(self, columns: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = set(columns)

    def filter(self, msg):
        return {k: v for k, v in msg.items() if k in self.columns}
