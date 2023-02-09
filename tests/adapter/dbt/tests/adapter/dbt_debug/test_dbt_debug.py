import pytest
import os
import re
import yaml

from dbt.cli.main import dbtUsageException
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
        for line in output.split("\n"):
            if linepat.match(line):
                found = True
                assert result in line
        if not found:
            with pytest.raises(Exception) as exc:
                msg = f"linepat {linepat} not found in stdout: {output}"
                assert msg in str(exc.value)

    def check_project(self, splitout, msg="ERROR invalid"):
        for line in splitout:
            if line.strip().startswith("dbt_project.yml file"):
                assert msg in line
            elif line.strip().startswith("profiles.yml file"):
                assert "ERROR invalid" not in line


class BaseDebugProfileVariable(BaseDebug):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "profile": '{{ "te" ~ "st" }}'}


class TestDebugPostgres(BaseDebug):
    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_nopass(self, project):
        run_dbt(["debug", "--target", "nopass"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+profiles\.yml file"), "ERROR invalid")

    def test_wronguser(self, project):
        run_dbt(["debug", "--target", "wronguser"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+Connection test"), "ERROR")

    def test_empty_target(self, project):
        run_dbt(["debug", "--target", "none_target"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), "misconfigured")


class TestDebugProfileVariablePostgres(BaseDebugProfileVariable):
    pass


class TestDebugInvalidProjectPostgres(BaseDebug):
    def test_empty_project(self, project):
        with open("dbt_project.yml", "w") as f:  # noqa: F841
            pass

        run_dbt(["debug", "--profile", "test"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)

    def test_badproject(self, project):
        update_project = {"invalid-key": "not a valid key so this is bad project"}

        with open("dbt_project.yml", "w") as f:
            yaml.safe_dump(update_project, f)

        run_dbt(["debug", "--profile", "test"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)

    def test_not_found_project(self, project):
        with pytest.raises(dbtUsageException):
            run_dbt(["debug", "--project-dir", "nopass"])

    def test_invalid_project_outside_current_dir(self, project):
        # create a dbt_project.yml
        project_config = {"invalid-key": "not a valid key in this project"}
        os.makedirs("custom", exist_ok=True)
        with open("custom/dbt_project.yml", "w") as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)
        run_dbt(["debug", "--project-dir", "custom"], expect_pass=False)
        splitout = self.capsys.readouterr().out.split("\n")
        self.check_project(splitout)
