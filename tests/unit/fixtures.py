from dbt.artifacts.resources import Contract, TestConfig, TestMetadata
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import (
    DependsOn,
    GenericTestNode,
    InjectedCTE,
    ModelConfig,
    ModelNode,
)
from dbt.node_types import NodeType


def model_node():
    return ModelNode(
        package_name="test",
        path="/root/models/foo.sql",
        original_file_path="models/foo.sql",
        language="sql",
        raw_code='select * from {{ ref("other") }}',
        name="foo",
        resource_type=NodeType.Model,
        unique_id="model.test.foo",
        fqn=["test", "models", "foo"],
        refs=[],
        sources=[],
        metrics=[],
        depends_on=DependsOn(),
        description="",
        primary_key=[],
        database="test_db",
        schema="test_schema",
        alias="bar",
        tags=[],
        config=ModelConfig(),
        contract=Contract(),
        meta={},
        compiled=True,
        extra_ctes=[InjectedCTE("whatever", "select * from other")],
        extra_ctes_injected=True,
        compiled_code="with whatever as (select * from other) select * from whatever",
        checksum=FileHash.from_contents(""),
        unrendered_config={},
    )


def generic_test_node():
    return GenericTestNode(
        package_name="test",
        path="/root/x/path.sql",
        original_file_path="/root/path.sql",
        language="sql",
        raw_code='select * from {{ ref("other") }}',
        name="foo",
        resource_type=NodeType.Test,
        unique_id="model.test.foo",
        fqn=["test", "models", "foo"],
        refs=[],
        sources=[],
        metrics=[],
        depends_on=DependsOn(),
        description="",
        database="test_db",
        schema="dbt_test__audit",
        alias="bar",
        tags=[],
        config=TestConfig(severity="warn"),
        contract=Contract(),
        meta={},
        compiled=True,
        extra_ctes=[InjectedCTE("whatever", "select * from other")],
        extra_ctes_injected=True,
        compiled_code="with whatever as (select * from other) select * from whatever",
        column_name="id",
        test_metadata=TestMetadata(namespace=None, name="foo", kwargs={}),
        checksum=FileHash.from_contents(""),
        unrendered_config={
            "severity": "warn",
        },
    )
