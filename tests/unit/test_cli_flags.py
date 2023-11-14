from multiprocessing import get_context
from pathlib import Path
from typing import List, Optional

import click
import pytest

from dbt.cli.exceptions import DbtUsageException
from dbt.cli.flags import Flags
from dbt.cli.main import cli
from dbt.cli.types import Command
from dbt.contracts.project import UserConfig
from dbt.exceptions import DbtInternalError
from dbt.helper_types import WarnErrorOptions
from dbt.tests.util import rm_file, write_file


class TestFlags:
    def make_dbt_context(
        self, context_name: str, args: List[str], parent: Optional[click.Context] = None
    ) -> click.Context:
        ctx = cli.make_context(context_name, args, parent)
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

        if "DEPRECATED_" in param.name.upper():
            assert not hasattr(flags, param.name.upper())
            return

        if param.name.upper() in ("VERSION", "LOG_PATH"):
            return

        assert hasattr(flags, param.name.upper())
        assert getattr(flags, param.name.upper()) == run_context.params[param.name.lower()]

    def test_log_path_default(self, run_context):
        flags = Flags(run_context)
        assert hasattr(flags, "LOG_PATH")
        assert getattr(flags, "LOG_PATH") == Path("logs")

    def test_log_file_max_size_default(self, run_context):
        flags = Flags(run_context)
        assert hasattr(flags, "LOG_FILE_MAX_BYTES")
        assert getattr(flags, "LOG_FILE_MAX_BYTES") == 10 * 1024 * 1024

    @pytest.mark.parametrize(
        "set_stats_param,do_not_track,expected_anonymous_usage_stats",
        [
            # set_stats_param = default, DNT = True, expected = False
            ("default", "1", False),
            ("default", "t", False),
            ("default", "true", False),
            ("default", "y", False),
            ("default", "yes", False),
            # set_stats_param = default, DNT = false, expected = True
            ("default", "false", True),
            ("default", "anything", True),
            # set_stats_param = True, DNT = True, expected = False
            (True, "1", False),
            (True, "t", False),
            (True, "true", False),
            (True, "y", False),
            (True, "yes", False),
            # set_stats_param = True, DNT = false, expected = True
            (True, "false", True),
            (True, "anything", True),
            (True, "2", True),
            # set_stats_param = False, DNT = True, expected = False
            (False, "1", False),
            (False, "t", False),
            (False, "true", False),
            (False, "y", False),
            (False, "yes", False),
            # set_stats_param = False, DNT = False, expected = False
            (False, "false", False),
            (False, "anything", False),
            (False, "2", False),
        ],
    )
    def test_anonymous_usage_state(
        self,
        monkeypatch,
        run_context,
        set_stats_param,
        do_not_track,
        expected_anonymous_usage_stats,
    ):
        monkeypatch.setenv("DO_NOT_TRACK", do_not_track)
        if set_stats_param != "default":
            run_context.params["send_anonymous_usage_stats"] = set_stats_param
        flags = Flags(run_context)
        assert flags.SEND_ANONYMOUS_USAGE_STATS == expected_anonymous_usage_stats

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

        with pytest.raises(DbtUsageException):
            Flags(context)

    @pytest.mark.parametrize("warn_error", [True, False])
    def test_mutually_exclusive_options_from_user_config(self, warn_error, user_config):
        user_config.warn_error = warn_error
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(DbtUsageException):
            Flags(context, user_config)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_envvar(self, warn_error, monkeypatch):
        monkeypatch.setenv("DBT_WARN_ERROR", warn_error)
        monkeypatch.setenv("DBT_WARN_ERROR_OPTIONS", '{"include":"all"}')
        context = self.make_dbt_context("run", ["run"])

        with pytest.raises(DbtUsageException):
            Flags(context)

    @pytest.mark.parametrize("warn_error", [True, False])
    def test_mutually_exclusive_options_from_cli_and_user_config(self, warn_error, user_config):
        user_config.warn_error = warn_error
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(DbtUsageException):
            Flags(context, user_config)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_cli_and_envvar(self, warn_error, monkeypatch):
        monkeypatch.setenv("DBT_WARN_ERROR", warn_error)
        context = self.make_dbt_context(
            "run", ["--warn-error-options", '{"include": "all"}', "run"]
        )

        with pytest.raises(DbtUsageException):
            Flags(context)

    @pytest.mark.parametrize("warn_error", ["True", "False"])
    def test_mutually_exclusive_options_from_user_config_and_envvar(
        self, user_config, warn_error, monkeypatch
    ):
        user_config.warn_error = warn_error
        monkeypatch.setenv("DBT_WARN_ERROR_OPTIONS", '{"include": "all"}')
        context = self.make_dbt_context("run", ["run"])

        with pytest.raises(DbtUsageException):
            Flags(context, user_config)

    @pytest.mark.parametrize(
        "cli_colors,cli_colors_file,flag_colors,flag_colors_file",
        [
            (None, None, True, True),
            (True, None, True, True),
            (None, True, True, True),
            (False, None, False, False),
            (None, False, True, False),
            (True, True, True, True),
            (False, False, False, False),
            (True, False, True, False),
            (False, True, False, True),
        ],
    )
    def test_no_color_interaction(
        self, cli_colors, cli_colors_file, flag_colors, flag_colors_file
    ):
        cli_params = []

        if cli_colors is not None:
            cli_params.append("--use-colors" if cli_colors else "--no-use-colors")

        if cli_colors_file is not None:
            cli_params.append("--use-colors-file" if cli_colors_file else "--no-use-colors-file")

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.USE_COLORS == flag_colors
        assert flags.USE_COLORS_FILE == flag_colors_file

    @pytest.mark.parametrize(
        "cli_log_level,cli_log_level_file,flag_log_level,flag_log_level_file",
        [
            (None, None, "info", "debug"),
            ("error", None, "error", "error"),  # explicit level overrides file level...
            ("info", None, "info", "info"),  # ...but file level doesn't change console level
            (
                "debug",
                "warn",
                "debug",
                "warn",
            ),  # still, two separate explicit levels are applied independently
        ],
    )
    def test_log_level_interaction(
        self, cli_log_level, cli_log_level_file, flag_log_level, flag_log_level_file
    ):
        cli_params = []

        if cli_log_level is not None:
            cli_params.append("--log-level")
            cli_params.append(cli_log_level)

        if cli_log_level_file is not None:
            cli_params.append("--log-level-file")
            cli_params.append(cli_log_level_file)

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.LOG_LEVEL == flag_log_level
        assert flags.LOG_LEVEL_FILE == flag_log_level_file

    @pytest.mark.parametrize(
        "cli_log_format,cli_log_format_file,flag_log_format,flag_log_format_file",
        [
            (None, None, "default", "debug"),
            ("json", None, "json", "json"),  # explicit format overrides file format...
            (None, "json", "default", "json"),  # ...but file format doesn't change console format
            (
                "debug",
                "text",
                "debug",
                "text",
            ),  # still, two separate explicit formats are applied independently
        ],
    )
    def test_log_format_interaction(
        self, cli_log_format, cli_log_format_file, flag_log_format, flag_log_format_file
    ):
        cli_params = []

        if cli_log_format is not None:
            cli_params.append("--log-format")
            cli_params.append(cli_log_format)

        if cli_log_format_file is not None:
            cli_params.append("--log-format-file")
            cli_params.append(cli_log_format_file)

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.LOG_FORMAT == flag_log_format
        assert flags.LOG_FORMAT_FILE == flag_log_format_file

    def test_log_settings_from_config(self):
        """Test that values set in UserConfig for log settings will set flags as expected"""
        context = self.make_dbt_context("run", ["run"])

        config = UserConfig(log_format="json", log_level="warn", use_colors=False)

        flags = Flags(context, config)

        assert flags.LOG_FORMAT == "json"
        assert flags.LOG_FORMAT_FILE == "json"
        assert flags.LOG_LEVEL == "warn"
        assert flags.LOG_LEVEL_FILE == "warn"
        assert flags.USE_COLORS is False
        assert flags.USE_COLORS_FILE is False

    def test_log_file_settings_from_config(self):
        """Test that values set in UserConfig for log *file* settings will set flags as expected, leaving the console
        logging flags with their default values"""
        context = self.make_dbt_context("run", ["run"])

        config = UserConfig(log_format_file="json", log_level_file="warn", use_colors_file=False)

        flags = Flags(context, config)

        assert flags.LOG_FORMAT == "default"
        assert flags.LOG_FORMAT_FILE == "json"
        assert flags.LOG_LEVEL == "info"
        assert flags.LOG_LEVEL_FILE == "warn"
        assert flags.USE_COLORS is True
        assert flags.USE_COLORS_FILE is False

    def test_duplicate_flags_raises_error(self):
        parent_context = self.make_dbt_context("parent", ["--version-check"])
        context = self.make_dbt_context("child", ["--version-check"], parent_context)

        with pytest.raises(DbtUsageException):
            Flags(context)

    def test_global_flag_at_child_context(self):
        parent_context_a = self.make_dbt_context("parent_context_a", ["--no-use-colors"])
        child_context_a = self.make_dbt_context("child_context_a", ["run"], parent_context_a)
        flags_a = Flags(child_context_a)

        parent_context_b = self.make_dbt_context("parent_context_b", ["run"])
        child_context_b = self.make_dbt_context(
            "child_context_b", ["--no-use-colors"], parent_context_b
        )
        flags_b = Flags(child_context_b)

        assert flags_a.USE_COLORS == flags_b.USE_COLORS

    def _create_flags_from_dict(self, cmd, d):
        write_file("", "profiles.yml")
        result = Flags.from_dict(cmd, d)
        assert result.which is cmd.value
        rm_file("profiles.yml")
        return result

    def test_from_dict__run(self):
        args_dict = {
            "print": False,
            "select": ["model_one", "model_two"],
        }
        result = self._create_flags_from_dict(Command.RUN, args_dict)
        assert "model_one" in result.select[0]
        assert "model_two" in result.select[1]

    def test_from_dict__build(self):
        args_dict = {
            "print": True,
            "state": "some/path",
        }
        result = self._create_flags_from_dict(Command.BUILD, args_dict)
        assert result.print is True
        assert "some/path" in str(result.state)

    def test_from_dict__seed(self):
        args_dict = {"use_colors": False, "exclude": ["model_three"]}
        result = self._create_flags_from_dict(Command.SEED, args_dict)
        assert result.use_colors is False
        assert "model_three" in result.exclude[0]

    def test_from_dict__which_fails(self):
        args_dict = {"which": "some bad command"}
        with pytest.raises(DbtInternalError, match=r"does not match value of which"):
            self._create_flags_from_dict(Command.RUN, args_dict)

    def test_from_dict_0_value(self):
        args_dict = {"log_file_max_bytes": 0}
        flags = Flags.from_dict(Command.RUN, args_dict)
        assert flags.LOG_FILE_MAX_BYTES == 0
