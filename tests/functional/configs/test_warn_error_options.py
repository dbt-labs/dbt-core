import pytest

from dbt.cli.main import dbtRunner
from dbt.events.types import DeprecatedModel
from tests.functional.utils import EventCatcher
from typing import Dict, Union

ModelsDictSpec = Dict[str, Union[str, "ModelsDictSpec"]]

my_model_sql = """SELECT 1 AS id, 'cats are cute' AS description"""
schema_yml = """
version: 2
models:
  - name: my_model
    deprecation_date: 2020-01-01
"""


class TestWarnErrorOptionsFromCLI:
    @pytest.fixture(scope="class")
    def models(self) -> ModelsDictSpec:
        return {"my_model.sql": my_model_sql, "schema.yml": schema_yml}

    def test_can_silence(self, project) -> None:
        catcher = EventCatcher(event_to_catch=DeprecatedModel)
        runner = dbtRunner(callbacks=[catcher.catch])
        runner.invoke(["run"])
        assert len(catcher.caught_events) == 1

        catcher.caught_events = []
        runner.invoke(
            ["run", "--warn-error-options", "{'include': 'all', 'silence': ['DeprecatedModel']}"]
        )
        assert len(catcher.caught_events) == 0
