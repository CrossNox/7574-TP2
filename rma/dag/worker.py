from copy import deepcopy
from typing import List, Optional

from rma.constants import (
    DEFAULT_PUBPORT,
    DEFAULT_REPPORT,
    DEFAULT_REQPORT,
    DEFAULT_SUBPORT,
)
from rma.dag.node import BASE_DATA, Node
from rma.dag.ventilator import VentilatorBlock


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

        self.deps: List[Node] = []

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
