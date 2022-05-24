# pylint: disable=pointless-statement

import abc
from copy import deepcopy
from typing import List, Optional

import yaml

DEFAULT_PUBPORT = 5000
DEFAULT_REPPORT = 6000
DEFAULT_SUBPORT = DEFAULT_PUBPORT
DEFAULT_REQPORT = DEFAULT_REPPORT

BASE_DATA = {
    "environment": ["PYTHONUNBUFFERED=1"],
    "networks": ["testing_net"],
    "image": "7574-tp2:latest",
    # "build": {"context": "..", "dockerfile": "docker/Dockerfile"},
    "volumes": [],
    "command": "",
}


class Node(abc.ABC):
    def __init__(
        self, node_id: str,
    ):
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.node_id = node_id
        self.ndeps = 0

    # TODO: shift lists!
    def __lshift__(self, other):
        if isinstance(other, Node):
            raise RuntimeError("Only nodes supported")
        self.parents.append(other)
        other.children.append(self)
        return self

    def __rshift__(self, other):
        if not isinstance(other, Node):
            raise RuntimeError("Only nodes supported")
        self.children.append(other)
        self.ndeps += 1
        other.parents.append(self)
        return other

    @property
    @abc.abstractmethod
    def config(self):
        pass


class DAG(Node):
    def __lshift__(self, other):
        raise RuntimeError("A DAG can not be a dependency")

    def __rshift__(self, other):
        if not isinstance(other, Source):
            raise RuntimeError("Only a source can be a dependency")
        return super().__rshift__(other)

    def _bfs(self):
        config_items = {}
        pending = [*self.children]
        while len(pending) > 0:
            next_pending = pending.pop()
            config_items.update(next_pending.config)
            pending.extend(next_pending.children)
        return config_items

    @property
    def config(self):
        services = self._bfs()
        data = {
            "version": "3",
            "services": services,
            "networks": {
                "testing_net": {
                    "ipam": {
                        "driver": "default",
                        "config": [{"subnet": "172.25.125.0/24"}],
                    }
                }
            },
        }
        return data


class Source(Node):
    def __init__(
        self,
        node_id: str,
        cmd: str,
        cmdargs: Optional[List[str]] = None,
        pubport: int = DEFAULT_PUBPORT,
        repport: int = DEFAULT_REPPORT,
        volumes: Optional[List[str]] = None,
    ):
        super().__init__(node_id)
        self.cmd = cmd
        self.cmdargs = cmdargs
        self.pubport = pubport
        self.repport = repport
        self.volumes = volumes or []

    def __lshift__(self, other):
        if not isinstance(other, DAG):
            raise RuntimeError("A Source can only depend on a DAG")
        return super().__lshift__(other)

    @property
    def config(self):
        data = {self.node_id: deepcopy(BASE_DATA)}
        data[self.node_id]["container_name"] = self.node_id
        _cmd = " ".join(self.cmdargs) if self.cmdargs is not None else ""
        data[self.node_id][
            "command"
        ] = f"source tcp://*:{self.pubport} tcp://*:{self.repport} {self.ndeps} {self.cmd} {_cmd}".strip()
        data[self.node_id]["volumes"] = self.volumes
        return data


V_SRC_PUSH_PORT = 8000
V_SRC_REP_PORT = 8001
V_SRC_REQ_PORT = 8002
V_W_PUSH_PORT = 8003


class VentilatorBlock(Node):
    def __init__(
        self,
        node_id: str,
        cmd: str,
        cmd_args: Optional[List[str]] = None,
        nworkers: int = 3,
        subport: int = DEFAULT_SUBPORT,
        reqport: int = DEFAULT_REQPORT,
        pubport: int = DEFAULT_PUBPORT,
        repport: int = DEFAULT_REPPORT,
    ):
        super().__init__(node_id)
        self.subport = subport
        self.reqport = reqport
        self.pubport = pubport
        self.repport = repport
        self.nworkers = nworkers

        self.cmd = cmd
        self.cmd_args = cmd_args or []

    @property
    def source_config(self):
        parent = self.parents[0]
        parent_name = parent.node_id
        if isinstance(parent, VentilatorBlock):
            parent_name = f"{parent.node_id}_sink"

        vsrc_name = f"{self.node_id}_src"
        data = {vsrc_name: deepcopy(BASE_DATA)}
        data[vsrc_name]["container_name"] = vsrc_name
        data[vsrc_name][
            "command"
        ] = f"ventilate source tcp://{parent_name}:{self.subport} tcp://{parent_name}:{self.reqport} tcp://*:{V_SRC_PUSH_PORT} tcp://*:{V_SRC_REP_PORT} tcp://{self.node_id}_sink:{V_SRC_REQ_PORT} {self.nworkers}"
        return data

    def worker_config(self, i):
        src_name = f"{self.node_id}_src"
        sink_name = f"{self.node_id}_sink"
        worker_name = f"{self.node_id}_{i}_worker"
        data = {worker_name: deepcopy(BASE_DATA)}
        _cmdargs = " ".join(self.cmd_args)
        data[worker_name]["container_name"] = worker_name
        data[worker_name][
            "command"
        ] = f"{self.cmd} tcp://{src_name}:{V_SRC_PUSH_PORT} tcp://{src_name}:{V_SRC_REP_PORT} tcp://{sink_name}:{V_W_PUSH_PORT} {_cmdargs}".strip()
        return data

    @property
    def sink_config(self):
        vsink_name = f"{self.node_id}_sink"
        data = {vsink_name: deepcopy(BASE_DATA)}
        data[vsink_name]["container_name"] = vsink_name
        data[vsink_name][
            "command"
        ] = f"ventilate sink tcp://*:{V_W_PUSH_PORT} tcp://*:{V_SRC_REQ_PORT} tcp://*:{self.pubport} {self.nworkers} tcp://*:{self.repport} {self.ndeps}"
        return data

    @property
    def config(self):
        data = {}
        data.update(self.source_config)
        for i in range(1, self.nworkers + 1):
            data.update(self.worker_config(i))
        data.update(self.sink_config)
        return data


class Worker(Node):
    def __init__(
        self,
        node_id: str,
        cmd: str,
        cmdargs: Optional[List[str]] = None,
        subport: int = DEFAULT_SUBPORT,
        reqport: int = DEFAULT_REQPORT,
        pubport: int = DEFAULT_PUBPORT,
        repport: int = DEFAULT_REPPORT,
    ):
        super().__init__(node_id)
        self.subport = subport
        self.reqport = reqport
        self.pubport = pubport
        self.repport = repport

        self.deps = []

        self.cmdargs = cmdargs or []
        self.cmd = cmd

    def __gt__(self, other):
        self.ndeps += 1
        other.deps.append(self)

    @property
    def config(self):
        parent = self.parents[0]
        parent_name = parent.node_id
        if isinstance(parent, VentilatorBlock):
            parent_name = f"{parent.node_id}_sink"

        data = {self.node_id: deepcopy(BASE_DATA)}
        data[self.node_id]["container_name"] = self.node_id

        _cmdargs = " ".join(self.cmdargs).strip()

        _deps_s = ""
        if len(self.deps) > 0:
            _deps = []
            for dep in self.deps:
                _deps.append(f"tcp://{dep.node_id}:{dep.repport}")
                _deps.append(f"tcp://{dep.node_id}:{dep.subport}")
            _deps_s = " ".join(_deps).strip()

        data[self.node_id][
            "command"
        ] = f"{self.cmd} tcp://{parent_name}:{self.subport} tcp://{parent_name}:{self.reqport} tcp://*:{self.pubport} tcp://*:{self.repport} {self.ndeps} {_cmdargs} {_deps_s}".strip()
        return data


class DAGJoiner(Node):
    def __init__(
        self,
        node_id: str,
        cmd: str,
        cmdargs: Optional[List[str]] = None,
        subport: int = DEFAULT_SUBPORT,
        reqport: int = DEFAULT_REQPORT,
        pubport: int = DEFAULT_PUBPORT,
        repport: int = DEFAULT_REPPORT,
    ):
        super().__init__(node_id)
        self.subport = subport
        self.reqport = reqport
        self.pubport = pubport
        self.repport = repport

        self.deps = []

        self.cmdargs = cmdargs or []
        self.cmd = cmd

    @property
    def config(self):
        data = {self.node_id: deepcopy(BASE_DATA)}

        _cmdargs = " ".join(self.cmdargs).strip()

        subaddrs = []
        reqaddrs = []
        for par in self.parents:
            par_node_id = par.node_id
            if isinstance(par, VentilatorBlock):
                par_node_id = f"{par.node_id}_sink"

            subaddrs.append("--subaddr")
            subaddrs.append(f"tcp://{par_node_id}:{par.pubport}")
            reqaddrs.append("--reqaddr")
            reqaddrs.append(f"tcp://{par_node_id}:{par.repport}")
        _subaddrs = " ".join(subaddrs)
        _reqaddrs = " ".join(reqaddrs)

        data[self.node_id]["container_name"] = self.node_id
        data[self.node_id][
            "command"
        ] = f"join {self.cmd} {_subaddrs} {_reqaddrs} tcp://*:{self.pubport} tcp://*:{self.repport} {self.ndeps} {_cmdargs}".strip()
        return data


class Sink(Node):
    def __init__(
        self,
        node_id,
        cmd,
        cmdargs: Optional[List[str]] = None,
        reqport: int = DEFAULT_REQPORT,
        subport: int = DEFAULT_SUBPORT,
        volumes: Optional[List[str]] = None,
    ):
        super().__init__(node_id)
        self.cmd = cmd
        self.cmdargs = cmdargs
        self.subport = subport
        self.reqport = reqport
        self.volumes = volumes or []

    def __rshift__(self, other):
        raise RuntimeError("Can't add a dependency to a sink")

    def __lshift__(self, other):
        if self.ndeps == 1:
            raise RuntimeError("Can only be dependency of one node")
        return super().__lshift__(other)

    @property
    def config(self):
        parent = self.parents[0]
        parent_name = parent.node_id
        if isinstance(parent, VentilatorBlock):
            parent_name = f"{parent.node_id}_sink"

        data = {self.node_id: deepcopy(BASE_DATA)}
        _cmd = " ".join(self.cmdargs).strip() if self.cmdargs is not None else ""
        data[self.node_id]["container_name"] = self.node_id
        data[self.node_id][
            "command"
        ] = f"sink tcp://{parent_name}:{self.subport} tcp://{parent_name}:{self.reqport} {self.cmd} {_cmd}".strip()
        data[self.node_id]["volumes"] = self.volumes
        return data


# ===================================================================== Start
dag = DAG("tp")
posts_source = Source(
    "posts_source_csv",
    "csv",
    ["/data/posts.csv"],
    volumes=[
        "../notebooks/data/the-reddit-irl-dataset-posts-reduced.csv:/data/posts.csv"
    ],
)
comments_source = Source(
    "comments_source_csv",
    "csv",
    ["/data/comments.csv"],
    volumes=[
        "../notebooks/data/the-reddit-irl-dataset-comments-reduced.csv:/data/comments.csv"
    ],
)
# ===================================================================== Posts top path
filter_posts_cols_top = VentilatorBlock(
    "filter_posts_cols_top", "transform filter-columns", ["id", "url"]
)
filter_null_url = VentilatorBlock("filter_null_url", "filter null-url")

# ===================================================================== Posts middle path
filter_posts_cols_middle = VentilatorBlock(
    "filter_cols1", "transform filter-columns", ["id", "score"]
)
posts_score_mean = Worker("posts_score_mean", "transform posts-score-mean")


# ===================================================================== Posts bottom
filter_posts_cols_bottom = VentilatorBlock(
    "filter_cols2", "transform filter-columns", ["score", "id", "url"]
)
filter_posts_above_mean_score = Worker(
    "filter_posts_above_mean_score", "filter posts-score-above-mean"
)

# ===================================================================== Comments bottom
filter_comments_cols_bottom = VentilatorBlock(
    "filter_comments_cols_bottom", "transform filter-columns", ["permalink", "body"]
)
filter_ed_comments = VentilatorBlock("filter_ed_comments", "filter ed-comments")
extract_post_id_bottom = VentilatorBlock(
    "extract_post_id_bottom", "transform extract-post-id"
)
filter_unique_posts = Worker("filter_unique_posts", "filter uniq-posts")

# ===================================================================== Comments side
filter_comments_cols_side = VentilatorBlock(
    "filter_comments_cols_side", "transform filter-columns", ["permalink", "sentiment"]
)
filter_nan_sentiment = VentilatorBlock("filter_nan_sentiment", "filter nan-sentiment")
extract_post_id_side = VentilatorBlock(
    "extract_post_id_side", "transform extract-post-id"
)
mean_sentiment = Worker("mean_sentiment", "transform mean-sentiment")

# ===================================================================== JOIN
join_dump_posts_urls = DAGJoiner("join_dump_posts_urls", "bykey", ["id"])
join_download_meme = DAGJoiner("join_download_meme", "bykey", ["id"])

# ===================================================================== Sink
sink = Sink("sink", "printmsg")
memes_url_sink = Sink("sink_memes_url", "printmsg")
mean_posts_score_sink = Sink("sink_mean_posts_score", "printmsg")
download_meme_sink = Sink("sink_download_meme", "printmsg")


dag >> posts_source
dag >> comments_source

posts_source >> filter_posts_cols_top >> filter_null_url >> join_download_meme
posts_source >> filter_posts_cols_middle >> posts_score_mean
posts_source >> filter_posts_cols_bottom >> filter_posts_above_mean_score
posts_score_mean > filter_posts_above_mean_score  # add dependency
filter_posts_above_mean_score >> join_dump_posts_urls

(
    comments_source
    >> filter_comments_cols_bottom
    >> filter_ed_comments
    >> extract_post_id_bottom
    >> filter_unique_posts
    >> join_dump_posts_urls
)

comments_source >> filter_comments_cols_side >> filter_nan_sentiment >> extract_post_id_side >> mean_sentiment >> join_download_meme

join_download_meme >> download_meme_sink
join_dump_posts_urls >> memes_url_sink
posts_score_mean >> mean_posts_score_sink


print(yaml.safe_dump(dag.config, indent=2, width=188))
