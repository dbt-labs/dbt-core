from typing import Any, Dict

import pytest
import yaml

from dbt.config.project import (
    VarProvider,
    validate_vars_not_in_both,
    vars_data_from_root,
)
from dbt.constants import VARS_FILE_NAME
from dbt.exceptions import DbtProjectError


class TestVarsDataFromRoot:
    """Tests for vars_data_from_root function."""

    def test_returns_empty_dict_when_file_missing(self, tmp_path) -> None:
        """Should return empty dict when vars.yml doesn't exist."""
        result = vars_data_from_root(str(tmp_path))
        assert result == {}

    def test_returns_empty_dict_when_file_empty(self, tmp_path) -> None:
        """Should return empty dict when vars.yml is empty."""
        vars_path = tmp_path / VARS_FILE_NAME
        vars_path.write_text("")
        result = vars_data_from_root(str(tmp_path))
        assert result == {}

    def test_returns_empty_dict_when_no_vars_key(self, tmp_path) -> None:
        """Should return empty dict when vars.yml has no 'vars' key."""
        vars_path = tmp_path / VARS_FILE_NAME
        vars_path.write_text("other_key: value\n")
        result = vars_data_from_root(str(tmp_path))
        assert result == {}

    def test_returns_vars_from_file(self, tmp_path) -> None:
        """Should return contents of 'vars' key from vars.yml."""
        vars_path = tmp_path / VARS_FILE_NAME
        vars_data = {
            "vars": {
                "my_var": "my_value",
                "another_var": 123,
            }
        }
        with open(vars_path, "w") as f:
            yaml.dump(vars_data, f)

        result = vars_data_from_root(str(tmp_path))
        assert result == {"my_var": "my_value", "another_var": 123}

    def test_returns_package_scoped_vars(self, tmp_path) -> None:
        """Should support package-scoped vars."""
        vars_path = tmp_path / VARS_FILE_NAME
        vars_data = {
            "vars": {
                "global_var": "global_value",
                "my_package": {
                    "package_var": "package_value",
                },
            }
        }
        with open(vars_path, "w") as f:
            yaml.dump(vars_data, f)

        result = vars_data_from_root(str(tmp_path))
        assert result == {
            "global_var": "global_value",
            "my_package": {"package_var": "package_value"},
        }


class TestValidateVarsNotInBoth:
    """Tests for validate_vars_not_in_both function."""

    def test_no_error_when_no_vars_file_and_no_project_vars(self) -> None:
        """Should not raise when neither vars.yml nor dbt_project.yml have vars."""
        project_dict: Dict[str, Any] = {"name": "test_project"}
        validate_vars_not_in_both("/test/path", project_dict, has_vars_file=False)

    def test_no_error_when_only_vars_file_has_vars(self) -> None:
        """Should not raise when only vars.yml has vars."""
        project_dict: Dict[str, Any] = {"name": "test_project"}
        validate_vars_not_in_both("/test/path", project_dict, has_vars_file=True)

    def test_no_error_when_only_project_has_vars(self) -> None:
        """Should not raise when only dbt_project.yml has vars."""
        project_dict: Dict[str, Any] = {
            "name": "test_project",
            "vars": {"my_var": "my_value"},
        }
        validate_vars_not_in_both("/test/path", project_dict, has_vars_file=False)

    def test_error_when_both_have_vars(self) -> None:
        """Should raise DbtProjectError when both sources have vars."""
        project_dict: Dict[str, Any] = {
            "name": "test_project",
            "vars": {"my_var": "my_value"},
        }
        with pytest.raises(DbtProjectError) as exc_info:
            validate_vars_not_in_both("/test/path", project_dict, has_vars_file=True)

        assert "vars.yml" in str(exc_info.value)
        assert "dbt_project.yml" in str(exc_info.value)
        assert "/test/path" in str(exc_info.value)

    def test_no_error_when_project_vars_is_empty(self) -> None:
        """Should not raise when dbt_project.yml vars is empty dict."""
        project_dict: Dict[str, Any] = {
            "name": "test_project",
            "vars": {},
        }
        # Empty vars dict is falsy, so this should not raise
        validate_vars_not_in_both("/test/path", project_dict, has_vars_file=True)

    def test_no_error_when_project_vars_is_none(self) -> None:
        """Should not raise when dbt_project.yml vars is None."""
        project_dict: Dict[str, Any] = {
            "name": "test_project",
            "vars": None,
        }
        validate_vars_not_in_both("/test/path", project_dict, has_vars_file=True)


class TestVarProviderWithVarsFromFile:
    """Tests for VarProvider with vars from file."""

    def test_vars_from_file_only(self) -> None:
        """VarProvider should work with vars from file."""
        vars_dict = {"my_var": "my_value", "another_var": 123}
        provider = VarProvider(vars_dict)
        assert provider.to_dict() == vars_dict

    def test_vars_from_file_with_package_scoped(self) -> None:
        """VarProvider should handle package-scoped vars from file."""
        vars_dict = {
            "global_var": "global_value",
            "my_package": {"package_var": "package_value"},
        }
        provider = VarProvider(vars_dict)
        assert provider.to_dict() == vars_dict
