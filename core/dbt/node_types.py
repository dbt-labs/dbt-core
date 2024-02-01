from typing import List

# preserving import path during dbt/artifacts refactor
from dbt.artifacts.resources.types import NodeType, AccessType, RunHookType, ModelLanguage  # noqa

EXECUTABLE_NODE_TYPES: List["NodeType"] = [
    NodeType.Model,
    NodeType.Test,
    NodeType.Snapshot,
    NodeType.Analysis,
    NodeType.Operation,
    NodeType.Seed,
    NodeType.Documentation,
    NodeType.RPCCall,
    NodeType.SqlOperation,
]

REFABLE_NODE_TYPES: List["NodeType"] = [
    NodeType.Model,
    NodeType.Seed,
    NodeType.Snapshot,
]

VERSIONED_NODE_TYPES: List["NodeType"] = [
    NodeType.Model,
]
