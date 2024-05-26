import os

import dbt_common.exceptions
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.task.base import BaseRunner, ConfiguredTask
from tests.unit.config import BaseConfigTest

INITIAL_ROOT = os.getcwd()


class MockRunner(BaseRunner):
    def compile(self):
        pass


class TestBaseRunner:
    def test_handle_generic_exception_handles_nodes_without_build_path(
        self, basic_parsed_source_definition_object: SourceDefinition
    ):
        # Source definition nodes don't have `build_path` attributes. Thus, this
        # test will fail if _handle_generic_exception doesn't account for this
        runner = MockRunner(
            config=None,
            adapter=None,
            node=basic_parsed_source_definition_object,
            node_index=None,
            num_nodes=None,
        )
        assert not hasattr(basic_parsed_source_definition_object, "build_path")
        runner._handle_generic_exception(Exception("bad thing happened"), ctx=None)


class InheritsFromConfiguredTask(ConfiguredTask):
    def run(self):
        pass


class TestConfiguredTask(BaseConfigTest):
    def tearDown(self):
        super().tearDown()
        # These tests will change the directory to the project path,
        # so it's necessary to change it back at the end.
        os.chdir(INITIAL_ROOT)

    def test_configured_task_dir_change(self):
        self.assertEqual(os.getcwd(), INITIAL_ROOT)
        self.assertNotEqual(INITIAL_ROOT, self.project_dir)
        InheritsFromConfiguredTask.from_args(self.args)
        self.assertEqual(os.path.realpath(os.getcwd()), os.path.realpath(self.project_dir))

    def test_configured_task_dir_change_with_bad_path(self):
        self.args.project_dir = "bad_path"
        with self.assertRaises(dbt_common.exceptions.DbtRuntimeError):
            InheritsFromConfiguredTask.from_args(self.args)
