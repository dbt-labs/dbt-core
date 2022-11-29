import pytest

import click
from multiprocessing import get_context

from dbt.cli.main import cli
from dbt.contracts.project import UserConfig
from dbt.cli.flags import Flags


class TestFlags:
    def make_dbt_context(self, args) -> click.Context:
        ctx = cli.make_context(cli.name, args)
        return ctx

    @pytest.fixture(scope="class")
    def run_context(self) -> click.Context:
        return self.make_dbt_context(["run"])

    def test_which(self, run_context):
        flags = Flags(run_context)
        assert flags.WHICH == "run"

    def test_mp_context(self, run_context):
        flags = Flags(run_context)
        assert flags.MP_CONTEXT == get_context("spawn")

    def test_cli_group_param_defaults(self, run_context):
        flags = Flags(run_context)
        for param in cli.params:
            assert hasattr(flags, param.name.upper())
            assert getattr(flags, param.name.upper()) == param.get_default(run_context)

    @pytest.mark.parametrize('do_not_track,expected_anonymous_usage_stats', [
        ("1", False),
        ("t", False),
        ("true", False),
        ("y", False),
        ("yes", False),
        ("false", True),
        ("anything", True),
        ("2", True),
    ])
    def test_anonymous_usage_state(self, monkeypatch, run_context, do_not_track, expected_anonymous_usage_stats):
        monkeypatch.setenv("DO_NOT_TRACK", do_not_track)

        flags = Flags(run_context)
        assert flags.ANONYMOUS_USAGE_STATS == expected_anonymous_usage_stats

    def test_empty_user_config_uses_default(self, run_context):
        user_config = UserConfig()

        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == run_context.params['use_colors']

    def test_none_user_config_uses_default(self, run_context):
        flags = Flags(run_context, None)
        assert flags.USE_COLORS == run_context.params['use_colors']

    def test_prefer_user_config_to_default(self, run_context):
        user_config = UserConfig(use_colors=False)
        # ensure default value is not the same as user config
        assert run_context.params['use_colors'] is not user_config.use_colors

        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == user_config.use_colors

    def test_prefer_param_value_to_user_config(self):
        user_config = UserConfig(use_colors=False)
        context = self.make_dbt_context(["--use-colors", "True", "run"])

        flags = Flags(context, user_config)
        assert flags.USE_COLORS
