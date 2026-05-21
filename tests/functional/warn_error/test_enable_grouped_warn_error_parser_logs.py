import pytest

from dbt.tests.util import run_dbt
from dbt_common.exceptions import EventCompilationError

SCHEMA_YML = """
version: 2

models:
  - name: missing_model
    description: no matching sql file
  - name: missing_model_two
    description: also missing
"""


@pytest.fixture(scope="class")
def models():
    return {
        "schema.yml": SCHEMA_YML,
    }


class TestEnableGroupedWarnErrorParserLogsFalse:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "enable_grouped_warn_error_parser_logs": False,
            },
        }

    def test_immediate_warn_error_only_first_in_message(self, project):
        with pytest.raises(EventCompilationError) as exc_info:
            run_dbt(["--warn-error", "parse"])
        msg = exc_info.value.msg
        assert "missing_model" in msg
        assert "missing_model_two" not in msg


class TestEnableGroupedWarnErrorParserLogsTrue:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "enable_grouped_warn_error_parser_logs": True,
            },
        }

    def test_deferred_warn_errors_combine_both_in_message(self, project):
        with pytest.raises(EventCompilationError) as exc_info:
            run_dbt(["--warn-error", "parse"])
        msg = exc_info.value.msg
        assert "missing_model" in msg
        assert "missing_model_two" in msg


class TestEnableGroupedWarnErrorParserLogsOmittedDefaultsToFalse:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {},
        }

    def test_omitted_flag_matches_false(self, project):
        with pytest.raises(EventCompilationError) as exc_info:
            run_dbt(["--warn-error", "parse"])
        msg = exc_info.value.msg
        assert "missing_model" in msg
        assert "missing_model_two" not in msg
