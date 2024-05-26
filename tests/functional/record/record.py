import os

from dbt.tests.util import run_dbt


class TestRecord:
    def test_record_when_env_var_set(self, project):
        temp = os.environ.get("DBT_RECORD", None)
        try:
            os.environ["DBT_RECORD"] = "True"
            run_dbt(["run"])
            assert os.path.isfile(os.path.join(os.getcwd(), "recording.json"))
        finally:
            if temp is None:
                del os.environ["DBT_RECORD"]
            else:
                os.environ["DBT_RECORD"] = temp
