import json

from rma.tasks.base import NonPreemptibleFilter


class FilterUniqPosts(NonPreemptibleFilter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_ids = set()

    def exec_step(self, msg):
        self.post_ids.add({"post_id": msg["post_id"]})

    def send_agg(self):
        for msg in self.post_ids:
            self.sender.send(json.dumps(msg).encode())
