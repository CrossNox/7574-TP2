import re

from rma.tasks.base import Transform


class ExtractPostID(Transform):
    def transform(self, msg):
        msg["post_id"] = re.match(
            r"https://old.reddit.com/r/me_?irl/comments/([^/]+)/.*", msg["permalink"]
        ).groups()[0]
        del msg["permalink"]
        return msg
