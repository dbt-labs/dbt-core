import pytest

from dbt.tests.util import run_dbt


my_model_sql = """
select 1 as fun
"""

seed_csv = """id,value
4,2
"""

somedoc_md = """
{% docs somedoc %}
Testing, testing
{% enddocs %}
"""

schema_yml = """
version: 2
models:
  - name: my_model
    description: testing model
"""


# Either a docs or a yml file is necessary to see the problem
# when two of the paths in 'all_source_paths' are the same
class TestDupeProjectPaths:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "seed.csv": seed_csv,
            "somedoc.md": somedoc_md,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "model-paths": ["models"],
            "seed-paths": ["models"],
        }

    def test_config_with_dupe_paths(self, project, dbt_project_yml):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1
