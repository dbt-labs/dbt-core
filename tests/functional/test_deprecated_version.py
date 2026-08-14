from io import StringIO
from unittest import mock

import pytest

import dbt.semver as semver
from dbt.deprecated_version import INFO_MSG, WARN_MSG
from dbt.events.base_types import EventLevel
from dbt.events.functions import capture_stdout_logs, stop_capture_stdout_logs
from dbt.main import parse_args
from dbt.tests.util import run_dbt

DEPRECATED_VERSION = "1.9.5"


class _StopInit(Exception):
    """Raised from a patched InitTask method so real profile setup never runs."""


def _deprecated_version() -> semver.VersionSpecifier:
    return semver.VersionSpecifier.from_version_string(DEPRECATED_VERSION)


def _capture(fn):
    """Run fn() with dbt's stdout log capture on, returning the captured text
    regardless of whether fn() raises."""
    stringbuf = StringIO()
    capture_stdout_logs(stringbuf)
    try:
        fn()
    finally:
        stop_capture_stdout_logs()
    return stringbuf.getvalue()


class TestVersionFlagWarnsNotInfo:
    def test_version_flag_fires_warn_not_info(self):
        # --version fires during argparse parsing, before handle_and_check()'s
        # own logging setup runs -- capture_stdout_logs() doesn't intercept
        # output from this early a point, so assert on fire_event directly
        # instead (same approach used for --version on the click-era branches).
        with mock.patch(
            "dbt.deprecated_version.get_installed_version", side_effect=_deprecated_version
        ), mock.patch("dbt.deprecated_version.fire_event") as fire_event_mock:
            with pytest.raises(SystemExit):
                parse_args(["--version"])

        fire_event_mock.assert_called_once()
        (fired_event,), kwargs = fire_event_mock.call_args
        assert type(fired_event).__name__ == "Note"
        assert kwargs.get("level") == EventLevel.WARN


class TestInitWarnsNotInfo:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_init_fires_warn_not_info(self, project):
        def invoke():
            with pytest.raises(_StopInit):
                run_dbt(["init", "--skip-profile-setup"], expect_pass=None)

        # check_deprecated_version() is the first line of InitTask.run(); stop
        # right after it, before any real profile/project scaffolding.
        with mock.patch(
            "dbt.deprecated_version.get_installed_version", side_effect=_deprecated_version
        ), mock.patch("dbt.task.init.InitTask.create_profiles_dir", side_effect=_StopInit):
            stdout = _capture(invoke)

        assert WARN_MSG in stdout
        assert INFO_MSG not in stdout


class TestOtherCommandsInfoNotWarn:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_other_command_fires_info_not_warn(self, project):
        with mock.patch(
            "dbt.deprecated_version.get_installed_version", side_effect=_deprecated_version
        ):
            stdout = _capture(lambda: run_dbt(["debug"], expect_pass=None))

        assert INFO_MSG in stdout
        assert WARN_MSG not in stdout
