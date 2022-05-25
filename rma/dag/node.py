import abc
from typing import List

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
        self,
        node_id: str,
    ):
        self.children: List[Node] = []
        self.parents: List[Node] = []
        self.node_id = node_id
        self.ndeps = 0

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
