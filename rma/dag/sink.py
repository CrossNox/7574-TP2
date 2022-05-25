from copy import deepcopy
from typing import List, Optional

from rma.dag.node import BASE_DATA, Node
from rma.dag.ventilator import VentilatorBlock
from rma.constants import DEFAULT_REQPORT, DEFAULT_SUBPORT


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

    @property
    def config(self):
        if len(self.parents) > 1:
            raise RuntimeError("Sinks can only have one upstream parent")
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
