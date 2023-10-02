import os

from dbt.cli.main import dbtRunner
from dbt.events.base_types import EventLevel


def test_performance_report(project):

    resource_report_level = None

    def check_for_report(e):
        # If we see a ResourceReport event, record its level
        if e.info.name == "ResourceReport":
            nonlocal resource_report_level
            resource_report_level = e.info.level

    runner = dbtRunner(callbacks=[check_for_report])

    runner.invoke(["run"])

    # With not cli flag or env var set, ResourceReport should be debug level.
    assert resource_report_level == EventLevel.DEBUG

    try:
        os.environ["DBT_SHOW_RESOURCE_REPORT"] = "1"
        runner.invoke(["run"])

        # With the appropriate env var set, ResourceReport should be info level.
        # This allows this fairly technical log line to be omitted by default
        # but still available in production scenarios.
        assert resource_report_level == EventLevel.INFO
    finally:
        del os.environ["DBT_SHOW_RESOURCE_REPORT"]
