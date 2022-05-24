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
        vsrc_name = f"{self.node_id}_src"
        data = {vsrc_name: deepcopy(BASE_DATA)}
        data[vsrc_name][
            "command"
        ] = f"ventilate source tcp://{parent.node_id}:{self.subport} tcp://{parent.node_id}:{self.reqport} tcp://*:{V_SRC_PUSH_PORT} tcp://*:{V_SRC_REP_PORT} tcp://{self.node_id}_sink:{V_SRC_REQ_PORT} {self.nworkers}"
        return data

    def worker_config(self, i):
        src_name = f"{self.node_id}_src"
        sink_name = f"{self.node_id}_sink"
        worker_name = f"{self.node_id}_worker_{i}"
        data = {worker_name: deepcopy(BASE_DATA)}
        _cmdargs = " ".join(self.cmd_args)
        data[worker_name][
            "command"
        ] = f"{self.cmd} tcp://{src_name}:{V_SRC_PUSH_PORT} tcp://{src_name}:{V_SRC_REP_PORT} tcp://{sink_name}:{V_W_PUSH_PORT} {_cmdargs}".strip()
        return data

    @property
    def sink_config(self):
        vsink_name = f"{self.node_id}_sink"
        data = {vsink_name: deepcopy(BASE_DATA)}
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
        data[self.node_id][
            "command"
        ] = f"sink tcp://{parent_name}:{self.subport} tcp://{parent_name}:{self.reqport} {self.cmd} {_cmd}".strip()
        data[self.node_id]["volumes"] = self.volumes
        return data


dag = DAG("tp")
source = Source(
    "source_csv",
    "csv",
    ["/data/posts.csv"],
    volumes=[
        "../notebooks/data/the-reddit-irl-dataset-posts-reduced.csv:/data/posts.csv"
    ],
)
filter_cols1 = VentilatorBlock(
    "filter_cols1", "transform filter-columns", ["id", "score"]
)
filter_cols2 = VentilatorBlock(
    "filter_cols2", "transform filter-columns", ["score", "id", "url"]
)

# filter_null_url = VentilatorBlock("filter_null_url", "filter null-url")

posts_score_mean = Worker("posts_score_mean", "transform posts-score-mean")
filter_posts_above_mean_score = Worker(
    "filter_posts_above_mean_score", "filter posts-score-above-mean"
)

sink = Sink("sink", "printmsg")

dag >> source
source >> filter_cols1 >> posts_score_mean
posts_score_mean > filter_posts_above_mean_score  # add dep
source >> filter_cols2 >> filter_posts_above_mean_score
filter_posts_above_mean_score >> sink

print(yaml.safe_dump(dag.config, indent=2, width=188))
