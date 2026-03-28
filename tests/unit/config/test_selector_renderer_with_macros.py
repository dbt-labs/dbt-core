from pathlib import Path

import pytest

from dbt.config.renderer import DbtProjectYamlRenderer
from dbt_common.exceptions import CompilationError


class TestSelectorRendererWithMacros:
    def test_render_selectors_with_root_project_macro(self, tmp_path: Path):
        macro_dir = tmp_path / "macros"
        macro_dir.mkdir()
        (macro_dir / "selector_for.sql").write_text(
            "{% macro selector_for(name) %}{{ return('tag:' ~ name) }}{% endmacro %}"
        )

        renderer = DbtProjectYamlRenderer(profile=None, cli_vars={})
        selectors = {
            "selectors": [
                {
                    "name": "dynamic_selector",
                    "definition": "{{ selector_for('nightly') }}",
                }
            ]
        }
        project = {"name": "test_project", "macro-paths": ["macros"]}

        rendered = renderer.render_selectors(
            selectors, project=project, project_root=str(tmp_path)
        )

        assert rendered["selectors"][0]["definition"] == "tag:nightly"

    def test_render_selectors_undefined_macro_without_project_context_errors(self):
        renderer = DbtProjectYamlRenderer(profile=None, cli_vars={})
        selectors = {
            "selectors": [
                {
                    "name": "dynamic_selector",
                    "definition": "{{ selector_for('nightly') }}",
                }
            ]
        }

        with pytest.raises(CompilationError, match="is undefined"):
            renderer.render_selectors(selectors)
