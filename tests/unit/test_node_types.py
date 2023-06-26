import pytest
from dbt.node_types import NodeType

node_type_pluralizations = {
    NodeType.Model: "models",
    NodeType.Analysis: "analyses",
    NodeType.Test: "tests",
    NodeType.Snapshot: "snapshots",
    NodeType.Operation: "operations",
    NodeType.Seed: "seeds",
    NodeType.RPCCall: "rpcs",
    NodeType.SqlOperation: "sql_operations",
    NodeType.Documentation: "docs",
    NodeType.Source: "sources",
    NodeType.Macro: "macros",
    NodeType.Exposure: "exposures",
    NodeType.Metric: "metrics",
    NodeType.Group: "groups",
    NodeType.SemanticModel: "semantic_models",
}


def test_all_types_have_pluralizations():
    assert set(NodeType) == set(node_type_pluralizations)


@pytest.mark.parametrize("node_type, pluralization", node_type_pluralizations.items())
def test_pluralizes_correctly(node_type, pluralization):
    assert node_type.pluralize() == pluralization
