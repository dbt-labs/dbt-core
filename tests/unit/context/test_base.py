import json
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

    def test_tojson_default_is_compact(self):
        result = BaseContext.tojson({"a": 1, "b": [2, 3]})
        # default: single line, no indent
        assert "\n" not in result
        assert json.loads(result) == {"a": 1, "b": [2, 3]}

    def test_tojson_with_indent(self):
        result = BaseContext.tojson({"a": 1}, indent=2)
        # indented: multi-line and starts with '{\n  '
        assert result == '{\n  "a": 1\n}'
        assert json.loads(result) == {"a": 1}

    def test_tojson_with_indent_and_sort_keys(self):
        result = BaseContext.tojson({"b": 2, "a": 1}, sort_keys=True, indent=4)
        assert result == '{\n    "a": 1,\n    "b": 2\n}'

    def test_toyaml_default(self):
        result = BaseContext.toyaml({"a": 1})
        # default safe_dump produces "a: 1\n"
        assert result == "a: 1\n"

    def test_toyaml_with_indent(self):
        result = BaseContext.toyaml({"a": {"b": 1}}, indent=4)
        # nested mapping should be indented by 4 spaces
        assert "\n    b: 1" in result
