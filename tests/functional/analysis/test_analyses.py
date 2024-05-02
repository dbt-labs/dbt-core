import os

import pytest

from dbt.tests.util import get_manifest, run_dbt

my_model_sql = """
select 1 as id
"""

raw_stuff_sql = """
{% raw %}
{% invalid jinja stuff %}
{% endraw %}
"""

schema_yml = """
version: 2

analyses:
  - name: my_analysis
    description: "This is my analysis"
"""

my_analysis_sql = """
select * from {{ ref('my_model') }}
"""


class TestAnalyses:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql}

    @pytest.fixture(scope="class")
    def analyses(self):
        return {
            "raw_stuff.sql": raw_stuff_sql,
            "schema.yml": schema_yml,
            "my_analysis.sql": my_analysis_sql,
        }

    def assert_contents_equal(self, path, expected):
        with open(path) as fp:
            assert fp.read().strip() == expected

    def test_postgres_analyses(self, project):
        compiled_analysis_path = os.path.normpath("target/compiled/test/analyses")
        path_1 = os.path.join(compiled_analysis_path, "my_analysis.sql")
        path_2 = os.path.join(compiled_analysis_path, "raw_stuff.sql")

        run_dbt(["clean"])
        assert not (os.path.exists(compiled_analysis_path))

        results = run_dbt(["compile"])
        assert len(results) == 3

        manifest = get_manifest(project.project_root)
        analysis_id = "analysis.test.my_analysis"
        assert analysis_id in manifest.nodes

        node = manifest.nodes[analysis_id]
        assert node.description == "This is my analysis"

        assert os.path.exists(path_1)
        assert os.path.exists(path_2)

        expected_sql = 'select * from "{}"."{}"."my_model"'.format(
            project.database, project.test_schema
        )
        self.assert_contents_equal(path_1, expected_sql)
        self.assert_contents_equal(path_2, "{% invalid jinja stuff %}")
