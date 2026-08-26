import os

from jinja2.runtime import Undefined
from opentelemetry import trace

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


class TestOtelIds:
    def test_defaults_to_zero_when_no_span_is_active(self):
        ctx = BaseContext(cli_vars={})
        assert ctx.otel_trace_id() == "0" * 32
        assert ctx.otel_span_id() == "0" * 16

    def test_reads_ids_from_the_active_span(self, otel_spans):
        ctx = BaseContext(cli_vars={})
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("test-span") as span:
            span_context = span.get_span_context()
            assert ctx.otel_trace_id() == format(span_context.trace_id, "032x")
            assert ctx.otel_span_id() == format(span_context.span_id, "016x")

    def test_span_id_tracks_the_innermost_span_while_trace_id_stays_fixed(self, otel_spans):
        # A pre/post-hook runs in its own nested span, distinct from the span
        # the model's main statement executes under; otel_span_id() must
        # follow whichever is currently active, while otel_trace_id() stays
        # the same throughout, since every span in a run shares one trace.
        ctx = BaseContext(cli_vars={})
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("outer") as outer_span:
            outer_trace_id = format(outer_span.get_span_context().trace_id, "032x")
            outer_span_id = format(outer_span.get_span_context().span_id, "016x")
            assert ctx.otel_span_id() == outer_span_id

            with tracer.start_as_current_span("inner") as inner_span:
                inner_span_id = format(inner_span.get_span_context().span_id, "016x")
                assert inner_span_id != outer_span_id
                assert ctx.otel_span_id() == inner_span_id
                assert ctx.otel_trace_id() == outer_trace_id

            assert ctx.otel_span_id() == outer_span_id
