import pytest

from dbt.tests.util import run_dbt

model_sql = "select 1 as id"

j2_suffixed_model_sql = "select 2 as id"

schema_yml = """
models:
  - name: model_sql
  - name: j2_suffixed_model_sql.sql
"""

doc_md = "{% docs doc_md %}test1{% enddocs %}"

j2_suffixed_doc_md = "{% docs j2_suffixed_doc_md %}test1{% enddocs %}"


class TestCustomSqlAndMdExtensionsParsed:
    """With sql_extensions and md_extensions configured, custom-extension files are parsed."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "sql-extensions": [".j2"],
            "md-extensions": [".j2"],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_sql.sql": model_sql,
            "j2_suffixed_model_sql.sql.j2": j2_suffixed_model_sql,
            "schema.yml": schema_yml,
            "doc_md.md": doc_md,
            "j2_suffixed_doc_md.md.j2": j2_suffixed_doc_md,
        }

    def test_custom_extensions_picked_up_by_parse(self, project):
        """Run dbt parse and assert model from .sql.j2 and doc from .md.j2 are in the manifest."""
        manifest = run_dbt(["parse"], expect_pass=True)

        expected_nodes = {
            "model.test.model_sql",
            "model.test.j2_suffixed_model_sql.sql",
        }
        expected_docs = {
            "doc.test.doc_md",
            "doc.test.j2_suffixed_doc_md",
        }
        assert expected_docs.issubset(set(manifest.docs.keys()))
        assert expected_nodes.issubset(set(manifest.nodes.keys()))
