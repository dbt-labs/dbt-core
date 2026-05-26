import pytest

from dbt.config.user_settings import (
    get_user_setting_flags,
    read_user_settings,
    set_user_setting_flag,
    write_user_settings,
)
from dbt.contracts.user_settings import UserSettings
from dbt_common.exceptions import DbtValidationError


class TestReadUserSettings:
    def test_missing_file_returns_defaults(self, tmp_path):
        result = read_user_settings(tmp_path / "nonexistent.yml")
        assert isinstance(result, UserSettings)
        assert result.flags == {}

    def test_empty_file_returns_defaults(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("")
        result = read_user_settings(p)
        assert isinstance(result, UserSettings)
        assert result.flags == {}

    def test_valid_yaml(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags:\n  send_anonymous_usage_stats: false\n")
        result = read_user_settings(p)
        assert result.flags == {"send_anonymous_usage_stats": False}

    def test_non_mapping_raises(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("- a\n- b\n")
        with pytest.raises(DbtValidationError, match="expected mapping"):
            read_user_settings(p)

    def test_invalid_yaml_raises(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text(":\n  :\n bad: [")
        with pytest.raises(DbtValidationError, match="invalid YAML"):
            read_user_settings(p)


class TestWriteUserSettings:
    def test_roundtrip(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        settings = UserSettings(flags={"fail_fast": True})
        write_user_settings(settings, p)
        result = read_user_settings(p)
        assert result.flags["fail_fast"] is True

    def test_creates_parent_dirs(self, tmp_path):
        p = tmp_path / "nested" / "dir" / "user_settings.yml"
        write_user_settings(UserSettings(), p)
        assert p.exists()


class TestGetUserSettingFlags:
    def test_returns_flags_dict(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags:\n  fail_fast: true\n")
        assert get_user_setting_flags(p) == {"fail_fast": True}

    def test_missing_file_returns_empty(self, tmp_path):
        assert get_user_setting_flags(tmp_path / "missing.yml") == {}

    def test_empty_flags_returns_empty(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags: {}\n")
        assert get_user_setting_flags(p) == {}

    def test_validation_error_returns_empty(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("- list\n- not mapping\n")
        assert get_user_setting_flags(p) == {}


class TestSetUserSettingFlag:
    def test_sets_new_flag(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags: {}\n")
        set_user_setting_flag("fail_fast", True, p)
        assert get_user_setting_flags(p) == {"fail_fast": True}

    def test_overwrites_existing_flag(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags:\n  fail_fast: false\n")
        set_user_setting_flag("fail_fast", True, p)
        assert get_user_setting_flags(p) == {"fail_fast": True}

    def test_creates_file_if_missing(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        set_user_setting_flag("fail_fast", True, p)
        assert get_user_setting_flags(p) == {"fail_fast": True}

    def test_unknown_flag_preserved(self, tmp_path):
        p = tmp_path / "user_settings.yml"
        p.write_text("flags: {}\n")
        set_user_setting_flag("custom_flag", True, p)
        assert get_user_setting_flags(p)["custom_flag"] is True
