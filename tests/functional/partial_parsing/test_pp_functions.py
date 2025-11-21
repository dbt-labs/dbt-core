import pytest

from dbt.artifacts.resources import FunctionArgument, FunctionReturns
from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import run_dbt, update_config_file, write_file
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.types import Note
from tests.functional.partial_parsing.fixtures import (
    model_using_function_sql,
    my_func_sql,
    my_func_yml,
    updated_my_func_sql,
    updated_my_func_yml,
)


class TestPartialParsingFunctions:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "my_func.sql": my_func_sql,
            "my_func.yml": my_func_yml,
        }

    def test_pp_functions(self, project):
        # initial run
        manifest = run_dbt(["parse"])
        assert isinstance(manifest, Manifest)
        assert len(manifest.functions) == 1
        function = manifest.functions["function.test.my_func"]
        assert function.raw_code == "value * 2"
        assert function.description == "Doubles an integer"
        assert function.arguments == [
            FunctionArgument(name="value", data_type="int", description="An integer to be doubled")
        ]
        assert function.returns == FunctionReturns(data_type="int")

        # update sql
        write_file(updated_my_func_sql, project.project_root, "functions", "my_func.sql")
        manifest = run_dbt(["parse"])
        assert isinstance(manifest, Manifest)
        assert len(manifest.functions) == 1
        function = manifest.functions["function.test.my_func"]
        assert function.raw_code == "number * 2.0"
        assert function.description == "Doubles an integer"
        assert function.arguments == [
            FunctionArgument(name="value", data_type="int", description="An integer to be doubled")
        ]
        assert function.returns == FunctionReturns(data_type="int")

        # update yml
        write_file(updated_my_func_yml, project.project_root, "functions", "my_func.yml")
        manifest = run_dbt(["parse"])
        assert isinstance(manifest, Manifest)
        assert len(manifest.functions) == 1
        function = manifest.functions["function.test.my_func"]
        assert function.raw_code == "number * 2.0"
        assert function.description == "Doubles a float"
        assert function.arguments == [
            FunctionArgument(name="number", data_type="float", description="A float to be doubled")
        ]
        assert function.returns == FunctionReturns(data_type="float")

        # if we parse again, partial parsing should be skipped
        note_catcher = EventCatcher(Note)
        manifest = run_dbt(["parse"], callbacks=[note_catcher.catch])
        assert isinstance(manifest, Manifest)
        assert len(manifest.functions) == 1
        assert len(note_catcher.caught_events) == 1
        assert (
            note_catcher.caught_events[0].info.msg == "Nothing changed, skipping partial parsing."
        )


class TestPartialParsingFunctionsAndCompilationOfDownstreamNodes:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "my_func.sql": my_func_sql,
            "my_func.yml": my_func_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_using_function.sql": model_using_function_sql,
        }

    def test_pp_functions(self, project):
        result = run_dbt(["compile"])
        # one function node and one model node
        assert len(result.results) == 2
        assert result.results[0].node.name == "my_func"
        assert result.results[0].node.config.alias is None
        assert result.results[1].node.name == "model_using_function"
        # `my_func` should be the third part of the name for the function in the compiled code
        assert "my_func" in result.results[1].node.compiled_code

        # Add an alias to `my_func`
        add_function_alias = {
            "functions": {
                "+alias": "aliased_my_func",
            }
        }
        update_config_file(add_function_alias, "dbt_project.yml")

        # Recompile
        result = run_dbt(["compile"])
        # one function node and one model node
        assert len(result.results) == 2
        assert result.results[0].node.name == "my_func"
        assert result.results[0].node.config.alias == "aliased_my_func"
        assert result.results[1].node.name == "model_using_function"
        # `aliased_my_func` should be the third part of the name for the function in the compiled code
        assert "aliased_my_func" in result.results[1].node.compiled_code
