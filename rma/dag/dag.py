import graphviz

from rma.dag.node import Node
from rma.dag.source import Source
from rma.dag.ventilator import VentilatorBlock


class DAG(Node):
    def __rshift__(self, other):
        if not isinstance(other, Source):
            raise RuntimeError("Only a source can be a dependency")
        return super().__rshift__(other)

    def _bfs(self):
        visited_nodes = set()
        config_items = {}
        pending = [*self.children]
        while len(pending) > 0:
            next_pending = pending.pop()
            if next_pending.node_id in visited_nodes:
                continue
            visited_nodes.add(next_pending.node_id)
            next_config = next_pending.config
            config_items.update(next_config)
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
                    },
                    "name": "testing_net",
                }
            },
        }
        return data

    def plot(self, location):
        dot = graphviz.Digraph(
            "DAG",
            comment="DAG rendered to a docker-compose file",
            format="png",
            graph_attr={"rankdir": "TB"},
        )
        visited = set()
        pending = [*self.children]
        while len(pending) > 0:
            next_pending = pending.pop()
            if next_pending.node_id in visited:
                continue

            visited.add(next_pending.node_id)

            node_kwargs = {}
            if isinstance(next_pending, VentilatorBlock):
                node_kwargs["style"] = "dotted"

            dot.node(next_pending.node_id, _attributes=node_kwargs)

            for parent in next_pending.parents:
                dot.edge(parent.node_id, next_pending.node_id)

            try:
                for dep in next_pending.deps:
                    dot.edge(
                        dep.node_id,
                        next_pending.node_id,
                        _attributes={"style": "dashed"},
                    )
            except AttributeError:
                pass

            pending.extend(next_pending.children)

        dot.render(directory=location)
