from unittest import mock

from dbt.artifacts.resources import DependsOn, UnitTestConfig
from dbt.contracts.graph.nodes import NodeType, UnitTestDefinition
from dbt.contracts.graph.unparsed import UnitTestOutputFixture
from dbt.parser import SchemaParser
from dbt.parser.unit_tests import UnitTestParser
from tests.unit.parser.test_parser import SchemaParserTest, assertEqualNodes
from tests.unit.utils import MockNode

UNIT_TEST_MODEL_NOT_FOUND_SOURCE = """
unit_tests:
    - name: test_my_model_doesnt_exist
      model: my_model_doesnt_exist
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
"""


UNIT_TEST_SOURCE = """
unit_tests:
    - name: test_my_model
      model: my_model
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
"""


UNIT_TEST_VERSIONED_MODEL_SOURCE = """
unit_tests:
    - name: test_my_model_versioned
      model: my_model_versioned.v1
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
"""


UNIT_TEST_CONFIG_SOURCE = """
unit_tests:
    - name: test_my_model
      model: my_model
      config:
        tags: "schema_tag"
        meta:
          meta_key: meta_value
          meta_jinja_key: '{{ 1 + 1 }}'
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
"""


UNIT_TEST_MULTIPLE_SOURCE = """
unit_tests:
    - name: test_my_model
      model: my_model
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
    - name: test_my_model2
      model: my_model
      description: "unit test description"
      given: []
      expect:
        rows:
          - {a: 1}
"""


class UnitTestParserTest(SchemaParserTest):
    def setUp(self):
        super().setUp()
        my_model_node = MockNode(
            package="snowplow",
            name="my_model",
            config=mock.MagicMock(enabled=True),
            schema="test_schema",
            refs=[],
            sources=[],
            patch_path=None,
        )
        self.manifest.nodes = {my_model_node.unique_id: my_model_node}
        self.parser = SchemaParser(
            project=self.snowplow_project_config,
            manifest=self.manifest,
            root_project=self.root_project_config,
        )

    def file_block_for(self, data, filename):
        return super().file_block_for(data, filename, "unit_tests")

    def test_basic(self):
        block = self.yaml_block_for(UNIT_TEST_SOURCE, "test_my_model.yml")

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=1, unit_tests=1)
        unit_test = list(self.parser.manifest.unit_tests.values())[0]
        expected = UnitTestDefinition(
            name="test_my_model",
            model="my_model",
            resource_type=NodeType.Unit,
            package_name="snowplow",
            path=block.path.relative_path,
            original_file_path=block.path.original_file_path,
            unique_id="unit_test.snowplow.my_model.test_my_model",
            given=[],
            expect=UnitTestOutputFixture(rows=[{"a": 1}]),
            description="unit test description",
            overrides=None,
            depends_on=DependsOn(nodes=["model.snowplow.my_model"]),
            fqn=["snowplow", "my_model", "test_my_model"],
            config=UnitTestConfig(),
            schema="test_schema",
        )
        expected.build_unit_test_checksum()
        assertEqualNodes(unit_test, expected)

    def test_unit_test_config(self):
        block = self.yaml_block_for(UNIT_TEST_CONFIG_SOURCE, "test_my_model.yml")
        self.root_project_config.unit_tests = {
            "snowplow": {"my_model": {"+tags": ["project_tag"]}}
        }

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=1, unit_tests=1)
        unit_test = self.parser.manifest.unit_tests["unit_test.snowplow.my_model.test_my_model"]
        self.assertEqual(sorted(unit_test.config.tags), sorted(["schema_tag", "project_tag"]))
        self.assertEqual(unit_test.config.meta, {"meta_key": "meta_value", "meta_jinja_key": "2"})

    def test_unit_test_versioned_model(self):
        block = self.yaml_block_for(UNIT_TEST_VERSIONED_MODEL_SOURCE, "test_my_model.yml")
        my_model_versioned_node = MockNode(
            package="snowplow",
            name="my_model_versioned",
            config=mock.MagicMock(enabled=True),
            refs=[],
            sources=[],
            patch_path=None,
            version=1,
        )
        self.manifest.nodes[my_model_versioned_node.unique_id] = my_model_versioned_node

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=2, unit_tests=1)
        unit_test = self.parser.manifest.unit_tests[
            "unit_test.snowplow.my_model_versioned.v1.test_my_model_versioned"
        ]
        self.assertEqual(len(unit_test.depends_on.nodes), 1)
        self.assertEqual(unit_test.depends_on.nodes[0], "model.snowplow.my_model_versioned.v1")

    def test_multiple_unit_tests(self):
        block = self.yaml_block_for(UNIT_TEST_MULTIPLE_SOURCE, "test_my_model.yml")

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=1, unit_tests=2)
        for unit_test in self.parser.manifest.unit_tests.values():
            self.assertEqual(len(unit_test.depends_on.nodes), 1)
            self.assertEqual(unit_test.depends_on.nodes[0], "model.snowplow.my_model")
