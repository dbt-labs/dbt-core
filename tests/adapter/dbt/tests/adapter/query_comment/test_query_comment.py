import pytest
import io
import json
import re
from dbt.exceptions import RuntimeException
from dbt.version import __version__ as dbt_version
from dbt.logger import log_manager
from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.query_comment.fixtures import MACROS__MACRO_SQL, MODELS__X_SQL


class BaseDefaultQueryComments:
    def matches_comment(self, msg) -> bool:
        if not msg.startswith("/* "):
            return False
        # our blob is the first line of the query comments, minus the comment
        json_str = msg.split("\n")[0][3:-3]
        data = json.loads(json_str)
        return (
            data["app"] == "dbt"
            and data["dbt_version"] == dbt_version
            and data["node_id"] == "model.test.x"
        )

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "name": "query_comment", "macro-paths": ["macros"]}

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

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        self.initial_stdout = log_manager.stdout
        self.initial_stderr = log_manager.stderr
        self.stringbuf = io.StringIO()
        log_manager.set_output_stream(self.stringbuf)

    def run_get_json(self, expect_pass=True):
        res, raw_logs = run_dbt_and_capture(
            ["--debug", "--log-format=json", "run"], expect_pass=expect_pass
        )

        parsed_logs = []
        for line in raw_logs.split("\n"):
            try:
                log = json.loads(line)
            except ValueError:
                continue

            parsed_logs.append(log)

        # empty lists evaluate as False
        assert len(parsed_logs) > 0
        return parsed_logs

    def query_comment(self, model_name, log):
        # N.B: a temporary string replacement regex to strip the HH:MM:SS from the log line if present.
        # TODO: make this go away when structured logging is stable
        log_msg = re.sub(r"(?:[01]\d|2[0123]):(?:[012345]\d):(?:[012345]\d \| )", "", log["msg"])
        prefix = "On {}: ".format(model_name)
        if log_msg.startswith(prefix):
            msg = log_msg[len(prefix) :]
            if msg in {"COMMIT", "BEGIN", "ROLLBACK"}:
                return None
            return msg
        return None

    def run_assert_comments(self):
        logs = self.run_get_json()

        seen = False
        for log in logs:
            msg = self.query_comment("model.test.x", log)
            if msg is not None and self.matches_comment(msg):
                seen = True

        for log in logs:
            if seen:
                "Never saw a matching log message! Logs:\n{}".format("\n".join(log["msg"]))

    def test_comments(self, project):
        self.run_assert_comments()


# Base setup to be inherited #
class BaseQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "dbt\nrules!\n"}

    def matches_comment(self, msg) -> bool:
        return msg.startswith("/* dbt\nrules! */\n")


class BaseMacroQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ query_header_no_args() }}"}

    def matches_comment(self, msg) -> bool:
        start_with = "/* dbt macros\nare pretty cool */\n"
        return msg.startswith(start_with)


class BaseMacroArgsQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ return(ordered_to_json(query_header_args(target.name))) }}"}

    def matches_comment(self, msg) -> bool:
        expected_dct = {
            "app": "dbt++",
            "dbt_version": dbt_version,
            "macro_version": "0.1.0",
            "message": "blah: default2",
        }
        expected = "/* {} */\n".format(json.dumps(expected_dct, sort_keys=True))
        return msg.startswith(expected)


class BaseMacroInvalidQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ invalid_query_header() }}"}

    def run_assert_comments(self):
        with pytest.raises(RuntimeException):
            self.run_get_json(expect_pass=False)


class BaseNullQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": ""}

    def matches_comment(self, msg) -> bool:
        return not ("/*" in msg or "*/" in msg)


class BaseEmptyQueryComments(BaseDefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": ""}

    def matches_comment(self, msg) -> bool:
        return not ("/*" in msg or "*/" in msg)


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
