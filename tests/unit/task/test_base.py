from dbt.task.base import BaseRunner
from dbt.contracts.graph.nodes import SourceDefinition


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
