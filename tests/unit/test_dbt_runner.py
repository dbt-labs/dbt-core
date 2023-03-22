import pytest

from dbt.cli.main import dbtRunner, dbtUsageException
from unittest import mock


class TestDbtRunner:
    @pytest.fixture
    def dbt(self) -> dbtRunner:
        return dbtRunner()

    def test_group_invalid_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["--invalid-option"])

    def test_command_invalid_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["deps", "--invalid-option"])

    def test_command_mutually_exclusive_option(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["--warn-error", "--warn-error-options", '{"include": "all"}', "deps"])

    def test_invalid_command(self, dbt: dbtRunner) -> None:
        with pytest.raises(dbtUsageException):
            dbt.invoke(["invalid-command"])

    def test_invoke_version(self, dbt: dbtRunner) -> None:
        dbt.invoke(["--version"])

    def test_callbacks(self) -> None:
        mock_callback = mock.MagicMock()
        dbt = dbtRunner(callbacks=[mock_callback])
        # the `debug` command is one of the few commands wherein you don't need
        # to have a project to run it and it will emit events
        dbt.invoke(["debug"])
        mock_callback.assert_called()
