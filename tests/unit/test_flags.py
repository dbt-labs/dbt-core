import os
from unittest import TestCase
from argparse import Namespace
import pytest

from dbt import flags
from dbt.contracts.project import UserConfig
from dbt.graph.selector_spec import IndirectSelection
from dbt.helper_types import WarnErrorOptions

# Skip due to interface for flag updated
pytestmark = pytest.mark.skip


class TestFlags(TestCase):
    def setUp(self):
        self.args = Namespace()
        self.user_config = UserConfig()

    def test__flags(self):

        # use_experimental_parser
        self.user_config.use_experimental_parser = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_EXPERIMENTAL_PARSER, True)
        os.environ["DBT_USE_EXPERIMENTAL_PARSER"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_EXPERIMENTAL_PARSER, False)
        setattr(self.args, "use_experimental_parser", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_EXPERIMENTAL_PARSER, True)
        # cleanup
        os.environ.pop("DBT_USE_EXPERIMENTAL_PARSER")
        delattr(self.args, "use_experimental_parser")
        flags.USE_EXPERIMENTAL_PARSER = False
        self.user_config.use_experimental_parser = None

        # static_parser
        self.user_config.static_parser = False
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.STATIC_PARSER, False)
        os.environ["DBT_STATIC_PARSER"] = "true"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.STATIC_PARSER, True)
        setattr(self.args, "static_parser", False)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.STATIC_PARSER, False)
        # cleanup
        os.environ.pop("DBT_STATIC_PARSER")
        delattr(self.args, "static_parser")
        flags.STATIC_PARSER = True
        self.user_config.static_parser = None

        # warn_error
        self.user_config.warn_error = False
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR, False)
        os.environ["DBT_WARN_ERROR"] = "true"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR, True)
        setattr(self.args, "warn_error", False)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR, False)
        # cleanup
        os.environ.pop("DBT_WARN_ERROR")
        delattr(self.args, "warn_error")
        flags.WARN_ERROR = False
        self.user_config.warn_error = None

        # warn_error_options
        self.user_config.warn_error_options = '{"include": "all"}'
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR_OPTIONS, WarnErrorOptions(include="all"))
        os.environ["DBT_WARN_ERROR_OPTIONS"] = '{"include": []}'
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR_OPTIONS, WarnErrorOptions(include=[]))
        setattr(self.args, "warn_error_options", '{"include": "all"}')
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WARN_ERROR_OPTIONS, WarnErrorOptions(include="all"))
        # cleanup
        os.environ.pop("DBT_WARN_ERROR_OPTIONS")
        delattr(self.args, "warn_error_options")
        self.user_config.warn_error_options = None

        # write_json
        self.user_config.write_json = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WRITE_JSON, True)
        os.environ["DBT_WRITE_JSON"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WRITE_JSON, False)
        setattr(self.args, "write_json", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.WRITE_JSON, True)
        # cleanup
        os.environ.pop("DBT_WRITE_JSON")
        delattr(self.args, "write_json")

        # partial_parse
        self.user_config.partial_parse = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PARTIAL_PARSE, True)
        os.environ["DBT_PARTIAL_PARSE"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PARTIAL_PARSE, False)
        setattr(self.args, "partial_parse", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PARTIAL_PARSE, True)
        # cleanup
        os.environ.pop("DBT_PARTIAL_PARSE")
        delattr(self.args, "partial_parse")
        self.user_config.partial_parse = False

        # use_colors
        self.user_config.use_colors = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_COLORS, True)
        os.environ["DBT_USE_COLORS"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_COLORS, False)
        setattr(self.args, "use_colors", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.USE_COLORS, True)
        # cleanup
        os.environ.pop("DBT_USE_COLORS")
        delattr(self.args, "use_colors")

        # debug
        self.user_config.debug = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.DEBUG, True)
        os.environ["DBT_DEBUG"] = "True"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.DEBUG, True)
        os.environ["DBT_DEBUG"] = "False"
        setattr(self.args, "debug", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.DEBUG, True)
        # cleanup
        os.environ.pop("DBT_DEBUG")
        delattr(self.args, "debug")
        self.user_config.debug = None

        # log_format -- text, json, default
        self.user_config.log_format = "text"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.LOG_FORMAT, "text")
        os.environ["DBT_LOG_FORMAT"] = "json"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.LOG_FORMAT, "json")
        setattr(self.args, "log_format", "text")
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.LOG_FORMAT, "text")
        # cleanup
        os.environ.pop("DBT_LOG_FORMAT")
        delattr(self.args, "log_format")
        self.user_config.log_format = None

        # version_check
        self.user_config.version_check = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.VERSION_CHECK, True)
        os.environ["DBT_VERSION_CHECK"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.VERSION_CHECK, False)
        setattr(self.args, "version_check", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.VERSION_CHECK, True)
        # cleanup
        os.environ.pop("DBT_VERSION_CHECK")
        delattr(self.args, "version_check")

        # fail_fast
        self.user_config.fail_fast = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.FAIL_FAST, True)
        os.environ["DBT_FAIL_FAST"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.FAIL_FAST, False)
        setattr(self.args, "fail_fast", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.FAIL_FAST, True)
        # cleanup
        os.environ.pop("DBT_FAIL_FAST")
        delattr(self.args, "fail_fast")
        self.user_config.fail_fast = False

        # send_anonymous_usage_stats
        self.user_config.send_anonymous_usage_stats = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.SEND_ANONYMOUS_USAGE_STATS, True)
        os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.SEND_ANONYMOUS_USAGE_STATS, False)
        setattr(self.args, "send_anonymous_usage_stats", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.SEND_ANONYMOUS_USAGE_STATS, True)
        os.environ["DO_NOT_TRACK"] = "1"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.SEND_ANONYMOUS_USAGE_STATS, False)
        # cleanup
        os.environ.pop("DBT_SEND_ANONYMOUS_USAGE_STATS")
        os.environ.pop("DO_NOT_TRACK")
        delattr(self.args, "send_anonymous_usage_stats")

        # printer_width
        self.user_config.printer_width = 100
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PRINTER_WIDTH, 100)
        os.environ["DBT_PRINTER_WIDTH"] = "80"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PRINTER_WIDTH, 80)
        setattr(self.args, "printer_width", "120")
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.PRINTER_WIDTH, 120)
        # cleanup
        os.environ.pop("DBT_PRINTER_WIDTH")
        delattr(self.args, "printer_width")
        self.user_config.printer_width = None

        # indirect_selection
        self.user_config.indirect_selection = "eager"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Eager)
        self.user_config.indirect_selection = "cautious"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Cautious)
        self.user_config.indirect_selection = "buildable"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Buildable)
        self.user_config.indirect_selection = None
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Eager)
        os.environ["DBT_INDIRECT_SELECTION"] = "cautious"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Cautious)
        setattr(self.args, "indirect_selection", "cautious")
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.INDIRECT_SELECTION, IndirectSelection.Cautious)
        # cleanup
        os.environ.pop("DBT_INDIRECT_SELECTION")
        delattr(self.args, "indirect_selection")
        self.user_config.indirect_selection = None

        # quiet
        self.user_config.quiet = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.QUIET, True)
        # cleanup
        self.user_config.quiet = None

        # no_print
        self.user_config.no_print = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.NO_PRINT, True)
        # cleanup
        self.user_config.no_print = None

        # cache_selected_only
        self.user_config.cache_selected_only = True
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.CACHE_SELECTED_ONLY, True)
        os.environ["DBT_CACHE_SELECTED_ONLY"] = "false"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.CACHE_SELECTED_ONLY, False)
        setattr(self.args, "cache_selected_only", True)
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.CACHE_SELECTED_ONLY, True)
        # cleanup
        os.environ.pop("DBT_CACHE_SELECTED_ONLY")
        delattr(self.args, "cache_selected_only")
        self.user_config.cache_selected_only = False

        # target_path/log_path
        flags.set_from_args(self.args, self.user_config)
        self.assertIsNone(flags.LOG_PATH)
        os.environ["DBT_LOG_PATH"] = "a/b/c"
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.LOG_PATH, "a/b/c")
        setattr(self.args, "log_path", "d/e/f")
        flags.set_from_args(self.args, self.user_config)
        self.assertEqual(flags.LOG_PATH, "d/e/f")
        # cleanup
        os.environ.pop("DBT_LOG_PATH")
        delattr(self.args, "log_path")

    def test__flags_are_mutually_exclusive(self):
        # options from user config
        self.user_config.warn_error = False
        self.user_config.warn_error_options = '{"include":"all"}'
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        self.user_config.warn_error = None
        self.user_config.warn_error_options = None

        # options from args
        setattr(self.args, "warn_error", False)
        setattr(self.args, "warn_error_options", '{"include":"all"}')
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        delattr(self.args, "warn_error")
        delattr(self.args, "warn_error_options")

        # options from environment
        os.environ["DBT_WARN_ERROR"] = "false"
        os.environ["DBT_WARN_ERROR_OPTIONS"] = '{"include": []}'
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        os.environ.pop("DBT_WARN_ERROR")
        os.environ.pop("DBT_WARN_ERROR_OPTIONS")

        # options from user config + args
        self.user_config.warn_error = False
        setattr(self.args, "warn_error_options", '{"include":"all"}')
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        self.user_config.warn_error = None
        delattr(self.args, "warn_error_options")

        # options from user config + environ
        self.user_config.warn_error = False
        os.environ["DBT_WARN_ERROR_OPTIONS"] = '{"include": []}'
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        self.user_config.warn_error = None
        os.environ.pop("DBT_WARN_ERROR_OPTIONS")

        # options from args + environ
        setattr(self.args, "warn_error", False)
        os.environ["DBT_WARN_ERROR_OPTIONS"] = '{"include": []}'
        with pytest.raises(ValueError):
            flags.set_from_args(self.args, self.user_config)
        # cleanup
        delattr(self.args, "warn_error")
        os.environ.pop("DBT_WARN_ERROR_OPTIONS")
