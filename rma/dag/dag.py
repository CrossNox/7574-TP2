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
    "build": {"context": "..", "dockerfile": "docker/Dockerfile"},
    "volumes": [],
    "command": "",
}


class Node(abc.ABC):
    def __init__(
        self,
        node_id: str,
    ):
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.node_id = node_id

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
        ] = f"source tcp://*:{self.pubport} tcp://*:{self.repport} {len(self.children)} {self.cmd} {_cmd}".strip()
        data[self.node_id]["volumes"] = self.volumes
        return data


class VentilatorBlock(Node):
    pass


class Worker(Node):
    def __init__(
        self,
        node_id: str,
        cmdargs,
        cmd,
        subcmdargs,
        subcmd,
    ):
        super().__init__(node_id)
        self.cmdargs = cmdargs
        self.cmd = cmd
        self.subcmdargs = subcmdargs
        self.subcmd = subcmd

    @property
    def config(self):
        data = {self.node_id: deepcopy(BASE_DATA)}
        _cmd = " ".join(self.cmdargs)
        _subcmd = " ".join(self.subcmdargs)
        data[self.node_id]["command"] = f"{_cmd} {self.cmd} {_subcmd} {self.subcmd}"
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
        if len(self.children) == 1:
            raise RuntimeError("Can only be dependency of one node")
        return super().__lshift__(other)

    @property
    def config(self):
        parent = self.parents[0]
        data = {self.node_id: deepcopy(BASE_DATA)}
        _cmd = " ".join(self.cmdargs).strip() if self.cmdargs is not None else ""
        data[self.node_id][
            "command"
        ] = f"sink tcp://{parent.node_id}:{self.subport} tcp://{parent.node_id}:{self.reqport} {self.cmd} {_cmd}".strip()
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
sink = Sink("sink", "printmsg")


dag >> source >> sink


print(yaml.safe_dump(dag.config))
