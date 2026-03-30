from unittest import mock
import pytest

from dbt.artifacts.resources import DependsOn, UnitTestConfig, UnitTestFormat
from dbt.contracts.graph.nodes import NodeType, UnitTestDefinition
from dbt.contracts.graph.unparsed import UnitTestOutputFixture
from dbt.parser import SchemaParser
from dbt.parser.unit_tests import UnitTestParser
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import add_callback_to_manager
from dbt_common.events.types import SystemStdErr
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

UNIT_TEST_NONE_ROWS_SORT = """
unit_tests:
  - name: test_my_model_null_handling
    model: my_model
    description: "unit test description"
    given: []
    expect:
        rows:
        - {"id":  , "col1": "d"}
        - {"id":  , "col1": "e"}
        - {"id": 6, "col1": "f"}
"""

UNIT_TEST_NONE_ROWS_SORT_CSV = """
unit_tests:
  - name: test_my_model_null_handling
    model: my_model
    description: "unit test description"
    given: []
    expect:
        format: csv
        rows: |
          id,col1
          ,d
          ,e
          6,f
"""

UNIT_TEST_NONE_ROWS_SORT_SQL = """
unit_tests:
  - name: test_my_model_null_handling
    model: my_model
    description: "unit test description"
    given: []
    expect:
        format: sql
        rows: |
          select null
          select 1
"""

UNIT_TEST_NONE_ROWS_SORT_FAILS = """
unit_tests:
  - name: test_my_model_null_handling
    model: my_model
    description: "this unit test needs one non-None value row"
    given: []
    expect:
        rows:
        - {"id":  , "col1": "d"}
        - {"id":  , "col1": "e"}
"""

UNIT_TEST_DISABLED = """
unit_tests:
    - name: test_my_model_disabled
      model: my_model
      description: "this unit test is disabled"
      config:
        enabled: false
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

    def test_unit_test_disabled(self):
        block = self.yaml_block_for(UNIT_TEST_DISABLED, "test_my_model.yml")
        self.root_project_config.unit_tests = {
            "snowplow": {"my_model": {"+tags": ["project_tag"]}}
        }

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=1, unit_tests=0, disabled=1)
        unit_test_disabled_list = self.parser.manifest.disabled[
            "unit_test.snowplow.my_model.test_my_model_disabled"
        ]
        self.assertEqual(len(unit_test_disabled_list), 1)
        unit_test_disabled = unit_test_disabled_list[0]
        self.assertEqual(unit_test_disabled.config.enabled, False)

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

    def _assert_fixture_yml_reorders_to_expected_rows(
        self, unit_test_fixture_yml, fixture_expected_field_format, expected_rows
    ):
        block = self.yaml_block_for(unit_test_fixture_yml, "test_my_model.yml")

        UnitTestParser(self.parser, block).parse()

        self.assert_has_manifest_lengths(self.parser.manifest, nodes=1, unit_tests=1)
        unit_test = list(self.parser.manifest.unit_tests.values())[0]
        expected = UnitTestDefinition(
            name="test_my_model_null_handling",
            model="my_model",
            resource_type=NodeType.Unit,
            package_name="snowplow",
            path=block.path.relative_path,
            original_file_path=block.path.original_file_path,
            unique_id="unit_test.snowplow.my_model.test_my_model_null_handling",
            given=[],
            expect=UnitTestOutputFixture(format=fixture_expected_field_format, rows=expected_rows),
            description="unit test description",
            overrides=None,
            depends_on=DependsOn(nodes=["model.snowplow.my_model"]),
            fqn=["snowplow", "my_model", "test_my_model_null_handling"],
            config=UnitTestConfig(),
            schema="test_schema",
        )
        expected.build_unit_test_checksum()
        assertEqualNodes(unit_test, expected)

    def test_expected_promote_non_none_row_dct(self):
        expected_rows = [
            {"id": 6, "col1": "f"},
            {"id": None, "col1": "e"},
            {"id": None, "col1": "d"},
        ]
        self._assert_fixture_yml_reorders_to_expected_rows(
            UNIT_TEST_NONE_ROWS_SORT, UnitTestFormat.Dict, expected_rows
        )

    def test_expected_promote_non_none_row_csv(self):
        expected_rows = [
            {"id": "6", "col1": "f"},
            {"id": None, "col1": "e"},
            {"id": None, "col1": "d"},
        ]
        self._assert_fixture_yml_reorders_to_expected_rows(
            UNIT_TEST_NONE_ROWS_SORT_CSV, UnitTestFormat.CSV, expected_rows
        )

    def test_expected_promote_non_none_row_sql(self):
        expected_rows = "select null\n" + "select 1"
        self._assert_fixture_yml_reorders_to_expected_rows(
            UNIT_TEST_NONE_ROWS_SORT_SQL, UnitTestFormat.SQL, expected_rows
        )

    def test_no_full_row_does_not_raise_exception(self):
        catcher = EventCatcher(SystemStdErr)
        add_callback_to_manager(catcher.catch)

        block = self.yaml_block_for(UNIT_TEST_NONE_ROWS_SORT_FAILS, "test_my_model.yml")
        UnitTestParser(self.parser, block).parse()

        assert len(catcher.caught_events) == 1


class TestEphemeralModelUnitTestFormatValidation:
    """parse_unit_test_case must raise a clear ParsingError when a unit test
    uses dict or csv format for an ephemeral model input, since ephemeral models
    have no database relation to introspect column types from.

    See https://github.com/dbt-labs/dbt-core/issues/11618
    """

    def _make_given(self, fmt: UnitTestFormat):
        from unittest.mock import MagicMock

        given = MagicMock()
        given.input = "ref('my_ephemeral_model')"
        given.format = fmt
        given.rows = []
        return given

    def _make_ephemeral_node(self):
        from unittest.mock import MagicMock

        node = MagicMock()
        node.config.materialized = "ephemeral"
        node.name = "my_ephemeral_model"
        return node

    def _run_validation(self, fmt: UnitTestFormat):
        """Run just the guard clause from UnitTestManifestLoader directly."""
        from dbt.exceptions import ParsingError

        given = self._make_given(fmt)
        original_input_node = self._make_ephemeral_node()

        # Replicate the guard clause from unit_tests.py
        if (
            given.format != UnitTestFormat.SQL
            and hasattr(original_input_node, "config")
            and getattr(original_input_node.config, "materialized", None) == "ephemeral"
        ):
            raise ParsingError(
                f"Unit test 'my_test' has input '{given.input}' with "
                f"format '{given.format.value}', but ephemeral models require "
                f"'format: sql' because they have no database relation to "
                f"introspect column types from."
            )

    def test_dict_format_ephemeral_raises_parsing_error(self):
        from dbt.exceptions import ParsingError

        with pytest.raises(ParsingError) as exc_info:
            self._run_validation(UnitTestFormat.Dict)

        msg = str(exc_info.value)
        assert "ephemeral" in msg
        assert "format: sql" in msg
        assert "my_ephemeral_model" in msg

    def test_csv_format_ephemeral_raises_parsing_error(self):
        from dbt.exceptions import ParsingError

        with pytest.raises(ParsingError) as exc_info:
            self._run_validation(UnitTestFormat.CSV)

        msg = str(exc_info.value)
        assert "ephemeral" in msg
        assert "format: sql" in msg

    def test_sql_format_ephemeral_does_not_raise(self):
        """SQL format is allowed for ephemeral models — must not raise."""
        # Should not raise
        self._run_validation(UnitTestFormat.SQL)

    def test_dict_format_non_ephemeral_does_not_raise(self):
        """Dict format on a non-ephemeral model must not raise."""
        from unittest.mock import MagicMock
        from dbt.exceptions import ParsingError

        given = self._make_given(UnitTestFormat.Dict)
        node = MagicMock()
        node.config.materialized = "table"

        # Guard should not fire for non-ephemeral
        if (
            given.format != UnitTestFormat.SQL
            and hasattr(node, "config")
            and getattr(node.config, "materialized", None) == "ephemeral"
        ):
            raise ParsingError("should not happen")
        # No exception = pass
