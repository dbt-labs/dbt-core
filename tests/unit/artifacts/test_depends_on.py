"""Regression tests for dbt-core#12900.

Fusion (dbt-fusion) adds extra keys such as `nodes_with_ref_location` to the
depends_on block of manifest nodes. DependsOn.__pre_deserialize__ must silently
drop those unknown keys so that loading a Fusion-generated manifest does not
raise InvalidFieldValue.
"""

import pytest

from dbt.artifacts.resources.v1.components import DependsOn, MacroDependsOn


class TestDependsOnFromDict:
    def test_extra_keys_stripped(self):
        data = {
            "macros": ["macro.pkg.my_macro"],
            "nodes": ["model.pkg.my_model"],
            "nodes_with_ref_location": [
                {"unique_id": "model.pkg.my_model", "ref_location": {"line": 1}}
            ],
        }
        result = DependsOn.from_dict(data)
        assert result.macros == ["macro.pkg.my_macro"]
        assert result.nodes == ["model.pkg.my_model"]

    def test_known_keys_preserved(self):
        data = {"macros": ["macro.a.b"], "nodes": ["model.a.c"]}
        result = DependsOn.from_dict(data)
        assert result.macros == ["macro.a.b"]
        assert result.nodes == ["model.a.c"]

    def test_empty_depends_on(self):
        result = DependsOn.from_dict({"macros": [], "nodes": []})
        assert result.macros == []
        assert result.nodes == []

    def test_multiple_unknown_keys_stripped(self):
        data = {
            "macros": [],
            "nodes": [],
            "nodes_with_ref_location": [],
            "future_unknown_field": "value",
        }
        result = DependsOn.from_dict(data)
        assert result.macros == []
        assert result.nodes == []

    def test_macro_depends_on_unaffected(self):
        # MacroDependsOn is the parent and should still work normally
        result = MacroDependsOn.from_dict({"macros": ["macro.pkg.m"]})
        assert result.macros == ["macro.pkg.m"]
