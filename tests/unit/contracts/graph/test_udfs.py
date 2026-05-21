from dataclasses import dataclass
from typing import Optional

import pytest

from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import FunctionNode, FunctionReturns, NodeType
from dbt.task.function import FunctionRunner


@dataclass
class StubRelation:
    database: Optional[str] = None
    schema: Optional[str] = None
    name: Optional[str] = None

    def __str__(self):
        parts = [self.database, self.schema, self.name]
        return ".".join(part for part in parts if part is not None)

    def include(self, database: bool = True, schema: bool = True, name: bool = True):
        if not database:
            self.database = None
        if not schema:
            self.schema = None
        if not name:
            self.name = None
        return self


@pytest.fixture
def function_node():
    return FunctionNode(
        resource_type=NodeType.Function,
        name="name",
        returns=FunctionReturns(data_type="int"),
        database="db",
        schema="schema",
        package_name="pkg",
        path="path/to/file.sql",
        original_file_path="path/to/original/file.sql",
        unique_id="pkg.schema.name",
        fqn=["pkg", "schema", "name"],
        alias="alias",
        checksum=FileHash.from_contents("test"),
    )


def test_function_node_description(mock_adapter, runtime_config, function_node):
    mock_adapter.Relation.create_from.return_value = StubRelation(
        database="db", schema="schema", name="name"
    )

    runner = FunctionRunner(
        config=runtime_config,
        adapter=mock_adapter,
        node=function_node,
        node_index=0,
        num_nodes=1,
    )

    assert runner.describe_node() == "function db.schema.name"


def test_function_node_description_with_default_database(
    mock_adapter, runtime_config, function_node
):
    mock_adapter.Relation.create_from.return_value = StubRelation(
        database=None, schema="schema", name="name"
    )

    function_node.database = runtime_config.credentials.database

    runner = FunctionRunner(
        config=runtime_config,
        adapter=mock_adapter,
        node=function_node,
        node_index=0,
        num_nodes=1,
    )

    assert runner.describe_node() == "function schema.name"


def test_function_node_is_relational(function_node):
    """FunctionNode.is_relational should return True so that function schemas
    are included in the on-run-end `schemas` variable (dbt-core#12516)."""
    assert function_node.is_relational is True


def test_function_node_is_not_refable(function_node):
    """Functions are not refable (they use {{ function() }} not {{ ref() }}),
    so is_refable should remain False even though is_relational is True."""
    assert function_node.is_refable is False
