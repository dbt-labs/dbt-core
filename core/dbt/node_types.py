from typing import List

# preserving import path during dbt/artifacts refactor
from dbt.artifacts.resources.types import (  # noqa
    AccessType,
    ModelLanguage,
    NodeType,
    RunHookType,
)

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
    NodeType.Function,
]

REFABLE_NODE_TYPES: List["NodeType"] = [
    NodeType.Model,
    NodeType.Seed,
    NodeType.Snapshot,
]

TEST_NODE_TYPES: List["NodeType"] = [
    NodeType.Test,
    NodeType.Unit,
]

VERSIONED_NODE_TYPES: List["NodeType"] = [
    NodeType.Model,
]
