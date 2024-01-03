import pytest

from dbt.common.helper_types import IncludeExclude, WarnErrorOptions
from dbt.common.dataclass_schema import ValidationError


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
        ],
    )
    def test_includes(self, include, exclude, expected_includes):
        include_exclude = IncludeExclude(include=include, exclude=exclude)

        assert include_exclude.includes("ItemA") == expected_includes


class TestWarnErrorOptions:
    def test_init_invalid_error(self):
        with pytest.raises(ValidationError):
            WarnErrorOptions(include=["InvalidError"])

        with pytest.raises(ValidationError):
            WarnErrorOptions(include="*", exclude=["InvalidError"])

    @pytest.mark.parametrize(
        "valid_error_name",
        [
            "NoNodesForSelectionCriteria",  # core event
            "AdapterDeprecationWarning",  # adapter event
            "RetryExternalCall",  # common event
        ],
    )
    def test_init_valid_error(self, valid_error_name):
        warn_error_options = WarnErrorOptions(include=[valid_error_name])
        assert warn_error_options.include == [valid_error_name]
        assert warn_error_options.exclude == []

        warn_error_options = WarnErrorOptions(include="*", exclude=[valid_error_name])
        assert warn_error_options.include == "*"
        assert warn_error_options.exclude == [valid_error_name]
