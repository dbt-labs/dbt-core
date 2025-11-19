"""Unit tests for rendering dbt_project.yml with vars without defaults."""

import pytest

from dbt.config.renderer import DbtProjectYamlRenderer
from dbt.context.base import BaseContext


class TestRendererWithRequiredVars:
    """Test that DbtProjectYamlRenderer doesn't raise errors for missing vars"""

    def test_base_context_with_require_vars_false(self):
        """Test that BaseContext with require_vars=False returns None for missing vars"""
        context = BaseContext(cli_vars={}, require_vars=False)
        var_func = context.var

        # Missing var should return None when require_vars=False
        assert var_func("missing_var") is None

        # Missing var with default should return the default
        assert var_func("missing_var", "default_value") == "default_value"

        # Existing var should return the value
        context2 = BaseContext(cli_vars={"existing_var": "value"}, require_vars=False)
        var_func2 = context2.var
        assert var_func2("existing_var") == "value"

    def test_base_context_with_require_vars_true_raises_error(self):
        """Test that BaseContext with require_vars=True raises error for missing vars"""
        from dbt.exceptions import RequiredVarNotFoundError

        context = BaseContext(cli_vars={}, require_vars=True)
        var_func = context.var

        # Missing var should raise error when require_vars=True
        with pytest.raises(RequiredVarNotFoundError):
            var_func("missing_var")

    def test_dbt_project_yaml_renderer_doesnt_fail_on_missing_vars(self):
        """Test that DbtProjectYamlRenderer with require_vars=False can render configs with missing vars"""
        # Pass require_vars=False to enable lenient mode (used by dbt deps)
        renderer = DbtProjectYamlRenderer(profile=None, cli_vars={}, require_vars=False)

        # This project config uses a var without a default value
        project_dict = {
            "name": "test_project",
            "version": "1.0",
            "models": {"test_project": {"+dataset": "dqm_{{ var('my_dataset') }}"}},
        }

        # This should not raise an error in lenient mode
        rendered = renderer.render_data(project_dict)

        # The var should be rendered as None (which becomes "dqm_None" in the string)
        assert "models" in rendered
        assert "test_project" in rendered["models"]
        # When var returns None, it gets stringified in the template
        assert rendered["models"]["test_project"]["+dataset"] == "dqm_None"

    def test_dbt_project_yaml_renderer_with_provided_var(self):
        """Test that DbtProjectYamlRenderer works correctly when var is provided"""
        renderer = DbtProjectYamlRenderer(profile=None, cli_vars={"my_dataset": "prod"})

        project_dict = {
            "name": "test_project",
            "version": "1.0",
            "models": {"test_project": {"+dataset": "dqm_{{ var('my_dataset') }}"}},
        }

        # This should render correctly with the provided var
        rendered = renderer.render_data(project_dict)

        # The var should be properly rendered
        assert rendered["models"]["test_project"]["+dataset"] == "dqm_prod"
