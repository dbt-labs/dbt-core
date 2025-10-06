import pytest

from dbt.artifacts.resources import FunctionArgument, FunctionReturns
from dbt.contracts.graph.manifest import Manifest
from dbt.tests.util import run_dbt, write_file
from dbt_common.events.types import Note
from tests.functional.partial_parsing.fixtures import (
    my_func_sql,
    my_func_yml,
    updated_my_func_sql,
    updated_my_func_yml,
)
from tests.utils import EventCatcher


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
