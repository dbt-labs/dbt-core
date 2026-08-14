from unittest import mock

import pytest

import dbt.semver as semver
from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.deprecated_version import INFO_MSG, WARN_MSG
from dbt.events.functions import capture_stdout_logs, stop_capture_stdout_logs
from dbt.main import parse_args
from dbt.tests.util import run_dbt

DEPRECATED_VERSION = "1.9.5"


class _StopInit(Exception):
    """Raised from a patched InitTask method so real profile setup never runs."""


def _deprecated_version() -> semver.VersionSpecifier:
    return semver.VersionSpecifier.from_version_string(DEPRECATED_VERSION)


def _restore_adapter(adapter):
    """`dbt init`/`dbt debug` register their own (incompatible) adapter under
    the same key, so the `project` fixture's teardown (which needs the
    original adapter to drop the test schema) fails afterward. Clear the
    registry and re-register + reload the macro manifest, mirroring what the
    `adapter` fixture itself does."""
    reset_adapters()
    register_adapter(adapter.config)
    get_adapter(adapter.config).load_macro_manifest(base_macros_only=True)


def _capture(fn):
    """Run fn() with dbt's stdout log capture on, returning the captured text
    regardless of whether fn() raises."""
    stringbuf = capture_stdout_logs()
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
        assert type(fired_event).__name__ == "DeprecatedVersionWarn"


class TestInitWarnsNotInfo:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_init_fires_warn_not_info(self, project, adapter):
        def invoke():
            with pytest.raises(_StopInit):
                run_dbt(["init", "--skip-profile-setup"], expect_pass=None)

        # check_deprecated_version() is the first line of InitTask.run(); stop
        # right after it, before any real profile/project scaffolding.
        try:
            with mock.patch(
                "dbt.deprecated_version.get_installed_version", side_effect=_deprecated_version
            ), mock.patch("dbt.task.init.InitTask.create_profiles_dir", side_effect=_StopInit):
                stdout = _capture(invoke)
        finally:
            _restore_adapter(adapter)

        assert WARN_MSG in stdout
        assert INFO_MSG not in stdout


class TestOtherCommandsInfoNotWarn:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_other_command_fires_info_not_warn(self, project, adapter):
        try:
            with mock.patch(
                "dbt.deprecated_version.get_installed_version", side_effect=_deprecated_version
            ):
                stdout = _capture(lambda: run_dbt(["debug"], expect_pass=True))
        finally:
            _restore_adapter(adapter)

        assert INFO_MSG in stdout
        assert WARN_MSG not in stdout
