from copy import deepcopy
from typing import List, Optional

from rma.dag.node import BASE_DATA, Node
from rma.dag.ventilator import VentilatorBlock
from rma.constants import (
    DEFAULT_PUBPORT,
    DEFAULT_REPPORT,
    DEFAULT_REQPORT,
    DEFAULT_SUBPORT,
)


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
