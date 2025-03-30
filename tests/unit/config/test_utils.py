import pytest

from dbt.config.utils import (
    exclusive_primary_alt_value_setting,
    normalize_warn_error_options,
)
from dbt.exceptions import DbtExclusivePropertyUseError


class TestExclusivePrimaryAltValueSetting:
    @pytest.fixture(scope="class")
    def primary_key(self) -> str:
        return "key_a"

    @pytest.fixture(scope="class")
    def alt_key(self) -> str:
        return "key_b"

    @pytest.fixture(scope="class")
    def value(self) -> str:
        return "I LIKE CATS"

    def test_primary_set(self, primary_key: str, alt_key: str, value: str):
        test_dict = {primary_key: value}
        exclusive_primary_alt_value_setting(test_dict, primary_key, alt_key)
        assert test_dict.get(primary_key) == value
        assert test_dict.get(alt_key) is None

    def test_alt_set(self, primary_key: str, alt_key: str, value: str):
        test_dict = {alt_key: value}
        exclusive_primary_alt_value_setting(test_dict, primary_key, alt_key)
        assert test_dict.get(primary_key) == value

    def test_primary_and_alt_set(self, primary_key: str, alt_key: str, value: str):
        test_dict = {primary_key: value, alt_key: value}
        with pytest.raises(DbtExclusivePropertyUseError):
            exclusive_primary_alt_value_setting(test_dict, primary_key, alt_key)

    def test_neither_primary_nor_alt_set(self, primary_key: str, alt_key: str):
        test_dict = {}
        exclusive_primary_alt_value_setting(test_dict, primary_key, alt_key)
        assert test_dict.get(primary_key) is None
        assert test_dict.get(alt_key) is None


class TestNormalizeWarnErrorOptions:
    def test_primary_set(self):
        test_dict = {
            "error": ["SomeWarning"],
        }
        normalize_warn_error_options(test_dict)
        assert len(test_dict) == 1
        assert test_dict["include"] == ["SomeWarning"]

    def test_convert(self):
        test_dict = {"warn": None, "silence": None, "include": ["SomeWarning"]}
        normalize_warn_error_options(test_dict)
        assert test_dict["exclude"] == []
        assert test_dict["include"] == ["SomeWarning"]
        assert test_dict["silence"] == []

    def test_both_keys_set(self):
        test_dict = {
            "warn": ["SomeWarning"],
            "exclude": ["SomeWarning"],
        }
        with pytest.raises(DbtExclusivePropertyUseError):
            normalize_warn_error_options(test_dict)

    def test_empty_dict(self):
        test_dict = {}
        normalize_warn_error_options(test_dict)
        assert test_dict.get("include") is None
        assert test_dict.get("exclude") is None
