import shutil

import pytest

from dbt.artifacts.resources.types import NodeType
from dbt.cli.main import dbtRunner
from dbt.cli.types import Command
from dbt.events.types import NoNodesSelected
from dbt.tests.util import run_dbt
from tests.utils import EventCatcher

"""
Testing different commands against the happy path fixture

The general flow
1. Declare the commands to be tested
2. Write a paramaterized test ensure a given command reaches causes and associated desired state.
"""

# These are commands we're skipping as they don't make sense or don't work with the
# happy path fixture currently
commands_to_skip = {
    "clone",
    "generate",
    "server",
    "init",
    "list",
    "run-operation",
    "show",
    "snapshot",
    "freshness",
}

# Commands to happy path test
commands = [command.value for command in Command if command.value not in commands_to_skip]


class TestRunCommands:
    @pytest.fixture(scope="class", autouse=True)
    def drop_snapshots(self, happy_path_project, project_root: str) -> None:
        """The snapshots are erroring out, so lets drop them.

        Seems to be database related. Ideally snapshots should work in these tests. It's a bad sign that they don't. That
        may have more to do with our fixture setup than the source code though.

        Note: that the `happy_path_fixture_files` are a _class_ based fixture. Thus although this fixture _modifies_ the
        files available to the happy path project, it doesn't affect that fixture for tests in other test classes.
        """

        shutil.rmtree(f"{project_root}/snapshots")

    @pytest.mark.parametrize("dbt_command", [(command,) for command in commands])
    def test_run_commmand(
        self,
        happy_path_project,
        dbt_command,
    ):
        run_dbt([dbt_command])


"""
Testing command interactions with specific node types

The general flow
1. Declare resource (node) types to be tested
2. Write a parameterized test that ensures commands interact successfully with each resource type
"""

# TODO: Figure out which of these are just missing from the happy path fixture vs which ones aren't selectable
skipped_resource_types = {
    "analysis",
    "operation",
    "rpc",
    "sql_operation",
    "doc",
    "macro",
    "exposure",
    "group",
    "unit_test",
    "fixture",
}
resource_types = [
    node_type.value for node_type in NodeType if node_type.value not in skipped_resource_types
]


class TestSelectResourceType:
    @pytest.fixture(scope="function")
    def catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=NoNodesSelected)

    @pytest.fixture(scope="function")
    def runner(self, catcher: EventCatcher) -> dbtRunner:
        return dbtRunner(callbacks=[catcher.catch])

    @pytest.mark.parametrize("resource_type", resource_types)
    def test_select_by_resource_type(
        self,
        resource_type: str,
        happy_path_project,
        runner: dbtRunner,
        catcher: EventCatcher,
    ) -> None:
        runner.invoke(["list", "--select", f"resource_type:{resource_type}"])
        assert len(catcher.caught_events) == 0
