import os
from typing import Callable, List, Optional

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt.events.base_types import EventMsg
from dbt.tests.util import get_run_results


def assert_run_results_have_compiled_node_attributes(
    args: List[str], result: dbtRunnerResult
) -> None:
    commands_with_run_results = ["build", "compile", "docs", "run", "test"]
    if not [a for a in args if a in commands_with_run_results] or not result.success:
        return

    run_results = get_run_results(os.getcwd())
    for r in run_results["results"]:
        if r["unique_id"].startswith("model") and r["status"] == "success":
            assert "compiled_code" in r
            assert "compiled" in r


_STANDARD_ASSERTIONS = [assert_run_results_have_compiled_node_attributes]


class dbtTestRunner(dbtRunner):
    def __init__(
        self,
        manifest: Optional[Manifest] = None,
        callbacks: Optional[List[Callable[[EventMsg], None]]] = None,
        exit_assertions: Optional[List[Callable[[List[str], dbtRunnerResult], None]]] = None,
    ):
        self.exit_assertions = exit_assertions if exit_assertions else _STANDARD_ASSERTIONS
        super().__init__(manifest, callbacks)

    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        result = super().invoke(args, **kwargs)

        for assertion in self.exit_assertions:
            assertion(args, result)

        return result
