from copy import deepcopy
from typing import List, Optional

from rma.constants import (
    V_W_PUSH_PORT,
    V_SRC_REP_PORT,
    V_SRC_REQ_PORT,
    DEFAULT_PUBPORT,
    DEFAULT_REPPORT,
    DEFAULT_REQPORT,
    DEFAULT_SUBPORT,
    V_SRC_PUSH_PORT,
)
from rma.dag.node import BASE_DATA, Node


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
