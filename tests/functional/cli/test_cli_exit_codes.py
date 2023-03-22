import pytest

from dbt.cli.main import cli
from dbt.cli.requires import HandledExit


good_sql = """
select 1 as fun
"""

bad_sql = """
someting bad
"""


class CliRunnerBase:
    def run_cli(self):
        ctx = cli.make_context(cli.name, ["run"])
        return cli.invoke(ctx)


class TestExitCodeZero(CliRunnerBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": good_sql}

    def test_no_exc_thrown(self, project):
        self.run_cli()


class TestExitCodeOne(CliRunnerBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model_one.sql": bad_sql}

    def test_exc_thrown(self, project):
        with pytest.raises(HandledExit):
            self.run_cli()
