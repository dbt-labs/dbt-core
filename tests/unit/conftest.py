import pytest

from dbt.contracts.graph.model_config import SourceConfig
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.contracts.graph.unparsed import Quoting
from dbt.node_types import NodeType


@pytest.fixture
def basic_parsed_source_definition_object():
    return SourceDefinition(
        columns={},
        database="some_db",
        description="",
        fqn=["test", "source", "my_source", "my_source_table"],
        identifier="my_source_table",
        loader="stitch",
        name="my_source_table",
        original_file_path="/root/models/sources.yml",
        package_name="test",
        path="/root/models/sources.yml",
        quoting=Quoting(),
        resource_type=NodeType.Source,
        schema="some_schema",
        source_description="my source description",
        source_name="my_source",
        unique_id="test.source.my_source.my_source_table",
        tags=[],
        config=SourceConfig(),
    )
