import pytest

from dbt.events.types import InvalidMacroAnnotation
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.ui import warning_tag

macros_sql = """
{% macro my_macro(my_arg_1, my_arg_2, my_arg_3) %}
{% endmacro %}
"""

bad_arg_names_macros_yml = """
macros:
  - name: my_macro
    description: This is the macro description.
    arguments:
      - name: my_arg_1
      - name: my_misnamed_arg_2
      - name: my_misnamed_arg_3
"""

bad_arg_count_macros_yml = """
macros:
  - name: my_macro
    arguments:
      - name: my_arg_1
        type: string
        description: This is an argument description.
"""

bad_arg_types_macros_yml = """
macros:
  - name: my_macro
    arguments:
      - name: my_arg_1
        type: string
      - name: my_arg_2
        type: invalid_type
      - name: my_arg_3
        type: int[int]
"""


bad_everything_types_macros_yml = """
macros:
  - name: my_macro
    arguments:
      - name: my_arg_1
        type: string
      - name: my_wrong_arg_2
        type: invalid_type
"""


class TestMacroDefaultArgMetadata:
    """Test that when the validate_macro_args behavior flag is enabled, macro
    argument names are included in the manifest even if there is no yml patch."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_macro_default_arg_metadata(self, project) -> None:
        manifest = run_dbt(["parse"])
        my_macro_args = manifest.macros["macro.test.my_macro"].arguments
        assert my_macro_args[0].name == "my_arg_1"
        assert my_macro_args[1].name == "my_arg_2"
        assert my_macro_args[2].name == "my_arg_3"


class TestMacroNameWarnings:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros_sql, "macros.yml": bad_arg_names_macros_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_macro_name_enforcement(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 2
        msg = "Argument my_misnamed_arg_2 in yaml for macro my_macro does not match the jinja"
        assert any(
            [e for e in event_catcher.caught_events if e.info.msg.startswith(warning_tag(msg))]
        )
        msg = "Argument my_misnamed_arg_3 in yaml for macro my_macro does not match the jinja"
        assert any(
            [e for e in event_catcher.caught_events if e.info.msg.startswith(warning_tag(msg))]
        )


class TestMacroTypeWarnings:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.sql": macros_sql, "macros.yml": bad_arg_types_macros_yml}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_macro_type_warnings(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 2
        msg = "Argument my_arg_2 in the yaml for macro my_macro has an invalid type"
        assert any(
            [e for e in event_catcher.caught_events if e.info.msg.startswith(warning_tag(msg))]
        )
        msg = "Argument my_arg_3 in the yaml for macro my_macro has an invalid type"
        assert any(
            [e for e in event_catcher.caught_events if e.info.msg.startswith(warning_tag(msg))]
        )


class TestMacroNonEnforcement:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"macros.yml": bad_everything_types_macros_yml, "macros.sql": macros_sql}

    def test_macro_non_enforcement(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0


# ── Generic test implicit-arg tests ──────────────────────────────────────────
# Generic tests always have 'model' and 'column_name' as implicit first args.
# Users don't document these in YAML, so validate_macro_args should not warn
# about them.

generic_test_with_extra_arg_sql = """
{% test my_custom_test(model, column_name, threshold) %}
  select 1
{% endtest %}
"""

generic_test_no_extra_args_sql = """
{% test my_simple_test(model, column_name) %}
  select 1
{% endtest %}
"""

# Only documents the non-implicit arg — correct usage.
generic_test_correct_yml = """
macros:
  - name: test_my_custom_test
    arguments:
      - name: threshold
        type: int
        description: Minimum threshold.
"""

# No args documented — correct for a test with no extra args.
generic_test_no_args_yml = """
macros:
  - name: test_my_simple_test
    description: A simple generic test.
    arguments: []
"""

# Wrong arg name for the non-implicit arg — should still warn.
generic_test_wrong_arg_name_yml = """
macros:
  - name: test_my_custom_test
    arguments:
      - name: wrong_arg_name
        type: int
"""

# Documents too many args (includes an extra one) — should warn about count.
generic_test_extra_arg_count_yml = """
macros:
  - name: test_my_custom_test
    arguments:
      - name: threshold
        type: int
      - name: unexpected_extra
        type: str
"""


class TestGenericTestImplicitArgsNoWarning:
    """validate_macro_args must not warn when a generic test's YAML omits the
    implicit 'model' and 'column_name' args (the normal, correct usage)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generic_test.sql": generic_test_with_extra_arg_sql,
            "generic_test.yml": generic_test_correct_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_no_warning_for_implicit_args(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0, (
            f"Expected no warnings for generic test implicit args, "
            f"got: {[e.info.msg for e in event_catcher.caught_events]}"
        )


class TestGenericTestNoExtraArgsNoWarning:
    """A generic test with no extra args and an empty YAML args list should
    produce no warnings."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generic_test.sql": generic_test_no_extra_args_sql,
            "generic_test.yml": generic_test_no_args_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_no_warning_for_test_with_no_extra_args(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 0, (
            f"Expected no warnings, "
            f"got: {[e.info.msg for e in event_catcher.caught_events]}"
        )


class TestGenericTestWrongArgNameStillWarns:
    """validate_macro_args must still warn when a non-implicit arg name is wrong."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generic_test.sql": generic_test_with_extra_arg_sql,
            "generic_test.yml": generic_test_wrong_arg_name_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_warns_for_wrong_non_implicit_arg_name(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) == 1
        assert "wrong_arg_name" in event_catcher.caught_events[0].info.msg


class TestGenericTestExtraArgCountStillWarns:
    """validate_macro_args must still warn when the YAML documents more
    non-implicit args than the Jinja definition has."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "generic_test.sql": generic_test_with_extra_arg_sql,
            "generic_test.yml": generic_test_extra_arg_count_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"validate_macro_args": True}}

    def test_warns_for_extra_arg_count(self, project) -> None:
        event_catcher = EventCatcher(event_to_catch=InvalidMacroAnnotation)
        run_dbt(["parse"], callbacks=[event_catcher.catch])
        assert len(event_catcher.caught_events) >= 1
        msgs = [e.info.msg for e in event_catcher.caught_events]
        assert any("number of arguments" in m for m in msgs)
