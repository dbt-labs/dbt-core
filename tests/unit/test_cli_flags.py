import pytest

import click
from multiprocessing import get_context
from typing import List

from dbt.cli.main import cli
from dbt.contracts.project import UserConfig
from dbt.cli.flags import Flags
from dbt.helper_types import WarnErrorOptions


class TestFlags:
    def make_dbt_context(self, context_name: str, args: List[str]) -> click.Context:
        ctx = cli.make_context(context_name, args)
        return ctx

    @pytest.fixture(scope="class")
    def run_context(self) -> click.Context:
        return self.make_dbt_context("run", ["run"])

    @pytest.fixture
    def user_config(self) -> UserConfig:
        return UserConfig()

    def test_which(self, run_context):
        flags = Flags(run_context)
        assert flags.WHICH == "run"

    def test_mp_context(self, run_context):
        flags = Flags(run_context)
        assert flags.MP_CONTEXT == get_context("spawn")

    @pytest.mark.parametrize("param", cli.params)
    def test_cli_group_flags_from_params(self, run_context, param):
        flags = Flags(run_context)
        if param.name.upper() in ("VERSION", "LOG_PATH"):
            return
        assert hasattr(flags, param.name.upper())
        assert getattr(flags, param.name.upper()) == run_context.params[param.name.lower()]

    def test_log_path_default(self, run_context):
        flags = Flags(run_context)
        assert hasattr(flags, "LOG_PATH")
        assert getattr(flags, "LOG_PATH") == "logs"

    @pytest.mark.parametrize(
        "do_not_track,expected_anonymous_usage_stats",
        [
            ("1", False),
            ("t", False),
            ("true", False),
            ("y", False),
            ("yes", False),
            ("false", True),
            ("anything", True),
            ("2", True),
        ],
    )
    def test_anonymous_usage_state(
        self, monkeypatch, run_context, do_not_track, expected_anonymous_usage_stats
    ):
        monkeypatch.setenv("DO_NOT_TRACK", do_not_track)

        flags = Flags(run_context)
        assert flags.ANONYMOUS_USAGE_STATS == expected_anonymous_usage_stats

    def test_empty_user_config_uses_default(self, run_context, user_config):
        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == run_context.params["use_colors"]

    def test_none_user_config_uses_default(self, run_context):
        flags = Flags(run_context, None)
        assert flags.USE_COLORS == run_context.params["use_colors"]

    def test_prefer_user_config_to_default(self, run_context, user_config):
        user_config.use_colors = False
        # ensure default value is not the same as user config
        assert run_context.params["use_colors"] is not user_config.use_colors

        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == user_config.use_colors

    def test_prefer_param_value_to_user_config(self):
        user_config = UserConfig(use_colors=False)
        context = self.make_dbt_context("run", ["--use-colors", "True", "run"])

        flags = Flags(context, user_config)
        assert flags.USE_COLORS

    def test_prefer_env_to_user_config(self, monkeypatch, user_config):
        user_config.use_colors = False
        monkeypatch.setenv("DBT_USE_COLORS", "True")
        context = self.make_dbt_context("run", ["run"])

        flags = Flags(context, user_config)
        assert flags.USE_COLORS

    def test_mutually_exclusive_options_passed_separately(self):
        """Assert options that are mutually exclusive can be passed separately without error"""
        warn_error_context = self.make_dbt_context("run", ["--warn-error", "run"])

        flags = Flags(warn_error_context)
        assert flags.WARN_ERROR

        warn_error_options_context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )
        flags = Flags(warn_error_options_context)
        assert flags.WARN_ERROR_OPTIONS == WarnErrorOptions(include="all")

    def test_mutually_exclusive_options_from_cli(self):
        context = self.make_dbt_context(
            "run", ["--warn-error", "--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(click.BadOptionUsage):
            Flags(context)

    @pytest.mark.parametrize("warn_error", [True, False])
    def test_mutually_exclusive_options_from_user_config(self, warn_error, user_config):
        user_config.warn_error = warn_error
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(click.BadOptionUsage):
            Flags(context, user_config)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_envvar(self, warn_error, monkeypatch):
        monkeypatch.setenv("DBT_WARN_ERROR", warn_error)
        monkeypatch.setenv("DBT_WARN_ERROR_OPTIONS", '{"include":"all"}')
        context = self.make_dbt_context("run", ["run"])

        with pytest.raises(click.BadOptionUsage):
            Flags(context)

    @pytest.mark.parametrize("warn_error", [True, False])
    def test_mutually_exclusive_options_from_cli_and_user_config(self, warn_error, user_config):
        user_config.warn_error = warn_error
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(click.BadOptionUsage):
            Flags(context, user_config)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_cli_and_envvar(self, warn_error, monkeypatch):
        monkeypatch.setenv("DBT_WARN_ERROR", warn_error)
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(click.BadOptionUsage):
            Flags(context)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_user_config_and_envvar(
        self, user_config, warn_error, monkeypatch
    ):
        user_config.warn_error = warn_error
        monkeypatch.setenv("DBT_WARN_ERROR_OPTIONS", '{"include": "all"}')
        context = self.make_dbt_context("run", ["run"])

        with pytest.raises(click.BadOptionUsage):
            Flags(context, user_config)
