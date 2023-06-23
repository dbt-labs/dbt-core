import pytest
import json
from dbt.exceptions import DbtRuntimeError
from dbt.version import __version__ as dbt_version
from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.query_comment.fixtures import MACROS__MACRO_SQL, MODELS__X_SQL


class BaseDefaultQueryComments:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "x.sql": MODELS__X_SQL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macro.sql": MACROS__MACRO_SQL,
        }

    def run_get_json(self, expect_pass=True):
        res, raw_logs = run_dbt_and_capture(
            ["--debug", "--log-format=json", "run"], expect_pass=expect_pass
        )

        # empty lists evaluate as False
        assert len(res) > 0
        return raw_logs

    def run_assert_comments(self):
        logs = self.run_get_json()
        return logs

    def test_comments(self, project):
        self.run_assert_comments()


# Base setup to be inherited #
class BaseQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "dbt\nrules!\n"}

    def matches_comment(self, logs) -> bool:
        assert "/* dbt\nrules! */\n" in logs


class BaseMacroQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ query_header_no_args() }}"}

    def matches_comment(self, logs) -> bool:
        assert "/* dbt macros\nare pretty cool */\n" in logs


class BaseMacroArgsQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ return(ordered_to_json(query_header_args(target.name))) }}"}

    def matches_comment(self, logs) -> bool:
        expected_dct = {
            "app": "dbt++",
            "dbt_version": dbt_version,
            "macro_version": "0.1.0",
            "message": "blah: default2",
        }
        expected = "/* {} */\n".format(json.dumps(expected_dct, sort_keys=True))
        assert expected in logs


class BaseMacroInvalidQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ invalid_query_header() }}"}

    def run_assert_comments(self):
        with pytest.raises(DbtRuntimeError):
            self.run_get_json(expect_pass=False)


class BaseNullQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": ""}

    def matches_comment(self, logs) -> bool:
        assert "/*" not in logs or "*/" not in logs


class BaseEmptyQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": ""}

    def matches_comment(self, logs) -> bool:
        assert "/*" not in logs or "*/" not in logs


# Tests #
class TestQueryComments(BaseQueryComments):
    pass


class TestMacroQueryComments(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryComments(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryComments(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryComments(BaseNullQueryComments):
    pass


class TestEmptyQueryComments(BaseEmptyQueryComments):
    pass
