from copy import deepcopy
from typing import List, Optional

from rma.constants import DEFAULT_PUBPORT, DEFAULT_REPPORT
from rma.dag.node import BASE_DATA, Node


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
