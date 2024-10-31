import os

from jinja2.runtime import Undefined

from dbt.context.base import BaseContext


class TestBaseContext:
    def test_log_jinja_undefined(self):
        # regression test for CT-2259
        try:
            os.environ["DBT_ENV_SECRET_LOG_TEST"] = "cats_are_cool"
            BaseContext.log(msg=Undefined(), info=True)
        except Exception as e:
            assert False, f"Logging an jinja2.Undefined object raises an exception: {e}"

    def test_log_with_dbt_env_secret(self):
        # regression test for CT-1783
        try:
            os.environ["DBT_ENV_SECRET_LOG_TEST"] = "cats_are_cool"
            BaseContext.log({"fact1": "I like cats"}, info=True)
        except Exception as e:
            assert False, f"Logging while a `DBT_ENV_SECRET` was set raised an exception: {e}"

    def test_flags(self):
        expected_context_flags = {
            "use_experimental_parser",
            "static_parser",
            "warn_error",
            "warn_error_options",
            "write_json",
            "partial_parse",
            "use_colors",
            "profiles_dir",
            "debug",
            "log_format",
            "version_check",
            "fail_fast",
            "send_anonymous_usage_stats",
            "printer_width",
            "indirect_selection",
            "log_cache_events",
            "quiet",
            "no_print",
            "cache_selected_only",
            "introspect",
            "target_path",
            "log_path",
            "invocation_command",
            "empty",
        }
        flags = BaseContext(cli_vars={}).flags
        for expected_flag in expected_context_flags:
            assert hasattr(flags, expected_flag.upper())
