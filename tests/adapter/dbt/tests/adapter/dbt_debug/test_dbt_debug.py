import pytest
import re
from dbt.tests.util import run_dbt

MODELS__MODEL_SQL = """
seled 1 as id
"""


class BaseDebug:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": MODELS__MODEL_SQL}

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    def assertGotValue(self, linepat, result):
        found = False
        output = self.capsys.readouterr().out
        for line in output.split('\n'):
            if linepat.match(line):
                found = True
                assert result in line
        if not found:
            with pytest.raises(Exception) as exc:
                msg = f'linepat {linepat} not found in stdout: {output}'
                assert msg in str(exc.value)


class BaseDebugProfileVariable(BaseDebug):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'profile': '{{ "te" ~ "st" }}'
        }


class TestDebug(BaseDebug):
    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_nopass(self, project):
        run_dbt(['debug', '--target', 'nopass'], expect_pass=False)
        self.assertGotValue(re.compile(r'\s+profiles\.yml file'), 'ERROR invalid')

    def test_wronguser(self, project):
        run_dbt(['debug', '--target', 'wronguser'], expect_pass=False)
        self.assertGotValue(re.compile(r'\s+Connection test'), 'ERROR')

    def test_empty_target(self, project):
        run_dbt(['debug', '--target', 'none_target'], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), 'misconfigured')


class TestDebugProfileVariablePostgres(BaseDebugProfileVariable):
    pass


# class TestDebugInvalidProjectPostgres(BaseDebug):
#     pass
