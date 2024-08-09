import os

import pytest

from dbt.tests.util import run_dbt

context_sql = """

{{
    config(
        materialized='table'
    )
}}

select
    '{{ env_var("local_user", "") }}' as lowercase,
    '{{ env_var("LOCAL_USER", "") }}' as uppercase,
    '{{ env_var("lOcAl_UsEr", "") }}' as mixedcase
"""


class TestEnvVars:
    @pytest.fixture(scope="class")
    def models(self):
        return {"context.sql": context_sql}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        os.environ["local_user"] = "dan"
        yield
        del os.environ["local_user"]

    def get_ctx_vars(self, project):
        fields = [
            "lowercase",
            "uppercase",
            "mixedcase",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = "select {field_list} from {schema}.context".format(
            field_list=field_list, schema=project.test_schema
        )
        vals = project.run_sql(query, fetch="all")
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])
        return ctx

    def test_env_vars(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1
        ctx = self.get_ctx_vars(project)

        # assert ctx["lowercase"] == "dan"

        # Windows env-vars are not case-sensitive, but Linux/macOS ones are
        # So on Windows, the uppercase and mixedcase vars should also resolve to "dan"
        if os.name == "nt":
            assert ctx["uppercase"] == "dan"
            # assert ctx["mixedcase"] == "dan"
        else:
            assert ctx["uppercase"] == ""
            assert ctx["mixedcase"] == ""
