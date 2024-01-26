from dataclasses import dataclass, field
from dbt.artifacts.resources.v1.macro import MacroDependsOn
from typing import List


@dataclass
class DependsOn(MacroDependsOn):
    nodes: List[str] = field(default_factory=list)

    def add_node(self, value: str):
        if value not in self.nodes:
            self.nodes.append(value)
