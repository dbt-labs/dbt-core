from dataclasses import dataclass
from typing import Optional

import pytest

from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import (
    FunctionArgument,
    FunctionNode,
    FunctionReturns,
    NodeType,
)
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


def _make_function_node(**kwargs):
    defaults = dict(
        resource_type=NodeType.Function,
        name="my_func",
        returns=FunctionReturns(data_type="integer"),
        database="db",
        schema="schema",
        package_name="pkg",
        path="path/to/file.sql",
        original_file_path="path/to/original/file.sql",
        unique_id="pkg.schema.my_func",
        fqn=["pkg", "schema", "my_func"],
        alias="my_func",
        checksum=FileHash.from_contents("test"),
        arguments=[FunctionArgument(name="a_string", data_type="string")],
    )
    defaults.update(kwargs)
    return FunctionNode(**defaults)


class TestFunctionNodeSameContents:
    def test_identical(self):
        node = _make_function_node()
        other = _make_function_node()
        assert node.same_contents(other, "postgres")

    def test_changed_arguments(self):
        node = _make_function_node()
        other = _make_function_node(
            arguments=[FunctionArgument(name="a_string", data_type="boolean")]
        )
        assert not node.same_contents(other, "postgres")

    def test_added_argument(self):
        node = _make_function_node()
        other = _make_function_node(
            arguments=[
                FunctionArgument(name="a_string", data_type="string"),
                FunctionArgument(name="b_int", data_type="integer"),
            ]
        )
        assert not node.same_contents(other, "postgres")

    def test_changed_returns(self):
        node = _make_function_node()
        other = _make_function_node(returns=FunctionReturns(data_type="string"))
        assert not node.same_contents(other, "postgres")

    def test_changed_body(self):
        node = _make_function_node(raw_code="SELECT 1")
        other = _make_function_node(raw_code="SELECT 2")
        assert not node.same_contents(other, "postgres")

    def test_same_schema(self):
        node = _make_function_node()
        other = _make_function_node()
        assert node.same_schema(other)

    def test_different_schema(self):
        node = _make_function_node()
        other = _make_function_node(returns=FunctionReturns(data_type="boolean"))
        assert not node.same_schema(other)
