
import pytest

from dbt.helper_types import IncludeExclude, WarnErrorOptions
from dbt.dataclass_schema import ValidationError


class TestIncludeExclude:
    def test_init_invalid(self):
        with pytest.raises(ValidationError):
            IncludeExclude(include="invalid")

        with pytest.raises(ValidationError):
            IncludeExclude(include=["ItemA"], exclude=["ItemB"])

    @pytest.mark.parametrize(
        "include,exclude,expected_includes",
        [
            ("all", [], True),
            ("*", [], True),
            ("*", ["ItemA"], False),
            (["ItemA"], [], True),
            (["ItemA", "ItemB"], [], True),
        ]
    )
    def test_includes(self, include, exclude, expected_includes):
        include_exclude = IncludeExclude(include=include, exclude=exclude)

        assert include_exclude.includes("ItemA") == expected_includes


class TestWarnErrorOptions:
    def test_init(self):
        with pytest.raises(ValidationError):
            WarnErrorOptions(include=["InvalidError"])

        with pytest.raises(ValidationError):
            WarnErrorOptions(include="*", exclude=["InvalidError"])

        warn_error_options = WarnErrorOptions(include=["NoNodesForSelectionCriteria"])
        assert warn_error_options.include == ["NoNodesForSelectionCriteria"]
        assert warn_error_options.exclude == []

        warn_error_options = WarnErrorOptions(include="*", exclude=["NoNodesForSelectionCriteria"])
        assert warn_error_options.include == "*"
        assert warn_error_options.exclude == ["NoNodesForSelectionCriteria"]
