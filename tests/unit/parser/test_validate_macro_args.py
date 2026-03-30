"""Unit tests for validate_macro_args generic test implicit-arg filtering.

Issue #11792: validate_macro_args=true with custom generic tests always warns
that number of macro args doesn't match, because the implicit 'model' and
'column_name' args are present in the Jinja definition but not in the YAML.
"""
from unittest import mock

import pytest

from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ParsedMacroPatch
from dbt.parser.schemas import MacroPatchParser


def _make_macro(name: str, arg_names: list) -> mock.Mock:
    """Return a lightweight mock Macro with the given name and argument list."""
    m = mock.Mock()
    m.name = name
    m.arguments = [MacroArgument(name=n) for n in arg_names]
    m.unique_id = f"macro.test.{name}"
    m.original_file_path = "macros/test.sql"
    return m


def _make_patch(arg_names: list) -> ParsedMacroPatch:
    """Return a ParsedMacroPatch with the given argument names."""
    return ParsedMacroPatch(
        name="irrelevant",
        description="",
        meta={},
        docs=mock.Mock(),
        config={},
        arguments=[MacroArgument(name=n) for n in arg_names],
        original_file_path="macros/test.yml",
        yaml_key="macros",
        package_name="test",
    )


def _call_check_patch_arguments(macro, patch):
    """Call MacroPatchParser._check_patch_arguments via unbound method (no parser instance needed)."""
    # We need a MacroPatchParser instance. The easiest approach: mock one that has
    # _fire_macro_arg_warning as a real method so we can capture calls.
    parser = mock.MagicMock(spec=MacroPatchParser)
    # Restore the real implementation of _check_patch_arguments so it runs.
    parser._check_patch_arguments = lambda m, p: MacroPatchParser._check_patch_arguments(
        parser, m, p
    )
    # Restore the real _fire_macro_arg_warning so it actually fires events.
    parser._fire_macro_arg_warning = lambda msg, m: MacroPatchParser._fire_macro_arg_warning(
        parser, msg, m
    )
    return parser


class TestCheckPatchArgumentsGenericTests:
    """_check_patch_arguments must strip implicit 'model'/'column_name' args for
    test_* macros before comparing against YAML-documented arguments."""

    def _run(self, macro, patch):
        """Run _check_patch_arguments and return a list of warning msgs fired."""
        fired = []

        class _FakeParser:
            _GENERIC_TEST_IMPLICIT_ARGS = MacroPatchParser._GENERIC_TEST_IMPLICIT_ARGS

            def _fire_macro_arg_warning(self, msg, m):
                fired.append(msg)

            _check_patch_arguments = MacroPatchParser._check_patch_arguments

        _FakeParser()._check_patch_arguments(macro, patch)
        return fired

    # ── Happy-path: no warnings expected ─────────────────────────────────────

    def test_generic_test_with_extra_arg_no_warning(self):
        """Jinja: [model, column_name, threshold]. YAML: [threshold]. No warning."""
        macro = _make_macro("test_my_custom", ["model", "column_name", "threshold"])
        patch = _make_patch(["threshold"])
        warnings = self._run(macro, patch)
        assert warnings == [], f"Expected no warnings, got: {warnings}"

    def test_generic_test_no_extra_args_no_warning(self):
        """Jinja: [model, column_name]. YAML: [] (empty). No warning."""
        macro = _make_macro("test_simple", ["model", "column_name"])
        patch = _make_patch([])
        # patch.arguments is empty, so _check_patch_arguments returns early.
        warnings = self._run(macro, patch)
        assert warnings == [], f"Expected no warnings, got: {warnings}"

    def test_generic_test_multiple_extra_args_no_warning(self):
        """Jinja: [model, column_name, arg1, arg2]. YAML: [arg1, arg2]. No warning."""
        macro = _make_macro("test_multi", ["model", "column_name", "arg1", "arg2"])
        patch = _make_patch(["arg1", "arg2"])
        warnings = self._run(macro, patch)
        assert warnings == [], f"Expected no warnings, got: {warnings}"

    # ── Regular macro: validation still fires ────────────────────────────────

    def test_regular_macro_arg_count_mismatch_warns(self):
        """Non-test_* macro with mismatched arg count must still warn."""
        macro = _make_macro("my_macro", ["arg1", "arg2", "arg3"])
        patch = _make_patch(["arg1"])
        warnings = self._run(macro, patch)
        assert any("number of arguments" in w for w in warnings), (
            f"Expected count-mismatch warning, got: {warnings}"
        )

    def test_regular_macro_arg_name_mismatch_warns(self):
        """Non-test_* macro with wrong arg name must still warn."""
        macro = _make_macro("my_macro", ["arg1", "arg2"])
        patch = _make_patch(["arg1", "wrong_name"])
        warnings = self._run(macro, patch)
        assert any("wrong_name" in w for w in warnings), (
            f"Expected name-mismatch warning, got: {warnings}"
        )

    # ── Generic test: wrong non-implicit arg still warns ─────────────────────

    def test_generic_test_wrong_extra_arg_name_warns(self):
        """Generic test with wrong non-implicit arg name must still warn."""
        macro = _make_macro("test_my_custom", ["model", "column_name", "threshold"])
        patch = _make_patch(["wrong_arg"])
        warnings = self._run(macro, patch)
        assert any("wrong_arg" in w for w in warnings), (
            f"Expected name-mismatch warning for wrong_arg, got: {warnings}"
        )

    def test_generic_test_extra_arg_count_warns(self):
        """Generic test where YAML documents more non-implicit args than Jinja must still warn."""
        macro = _make_macro("test_my_custom", ["model", "column_name", "threshold"])
        patch = _make_patch(["threshold", "unexpected_extra"])
        warnings = self._run(macro, patch)
        assert any("number of arguments" in w for w in warnings), (
            f"Expected count-mismatch warning, got: {warnings}"
        )
