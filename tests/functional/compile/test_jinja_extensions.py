from collections import Counter

import pytest

from dbt.tests.util import run_dbt

model_sql = "select 1 as id"

j2_suffixed_model_sql = "select 2 as id"

schema_yml = """
models:
  - name: model_sql
  - name: j2_suffixed_model_sql.sql
"""

versioned_model_sql_v1 = "select 1 as id"
versioned_model_sql_v2 = "select 2 as id"
versioned_j2_model_sql_v1 = "select 10 as id"
versioned_j2_model_sql_v2 = "select 20 as id"

schema_versioned_yml = """
models:
  - name: model_sql
    versions:
      - v: 1
      - v: 2
  - name: j2_suffixed_model_sql
    versions:
      - v: 1
      - v: 2
"""

doc_md = "{% docs doc_md %}test1{% enddocs %}"

j2_suffixed_doc_md = "{% docs j2_suffixed_doc_md %}test1{% enddocs %}"


class TestJinjaExtensionsParsedWhenFlagIsSet:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"allow_jinja_file_extensions": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_sql.sql": model_sql,
            "j2_suffixed_model_sql.sql.j2": j2_suffixed_model_sql,
            "schema.yml": schema_yml,
            "doc_md.md": doc_md,
            "j2_suffixed_doc_md.md.j2": j2_suffixed_doc_md,
        }

    def test_jinja_extensions_picked_up_by_parse(self, project):
        manifest = run_dbt(["parse"], expect_pass=True)

        expected_nodes = {
            "model.test.model_sql",
            "model.test.j2_suffixed_model_sql",
        }
        expected_docs = {
            "doc.test.doc_md",
            "doc.test.j2_suffixed_doc_md",
        }
        expected_node_names = {"model_sql", "j2_suffixed_model_sql"}
        actual_node_names = {node.name for node in manifest.nodes.values()}

        expected_doc_names = {"doc_md", "j2_suffixed_doc_md"}
        actual_doc_names = {doc.name for doc in manifest.docs.values()}

        assert expected_docs.issubset(set(manifest.docs.keys()))
        assert expected_nodes.issubset(set(manifest.nodes.keys()))

        assert expected_node_names.issubset(actual_node_names)
        assert expected_doc_names.issubset(actual_doc_names)


class TestJinjaExtensionsNotParsedWhenFlagIsNotSet:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_sql.sql": model_sql,
            "j2_suffixed_model_sql.sql.j2": j2_suffixed_model_sql,
            "schema.yml": schema_yml,
            "doc_md.md": doc_md,
            "j2_suffixed_doc_md.md.j2": j2_suffixed_doc_md,
        }

    def test_jinja_extensions_picked_up_by_parse(self, project):
        manifest = run_dbt(["parse"], expect_pass=True)

        expected_node = "model.test.model_sql"
        unexpected_node = "model.test.j2_suffixed_model_sql"
        expected_doc = "doc.test.doc_md"
        unexpected_doc = "doc.test.j2_suffixed_doc_md"

        assert expected_doc in manifest.docs
        assert expected_node in manifest.nodes
        assert unexpected_node not in manifest.nodes
        assert unexpected_doc not in manifest.docs


class TestJinjaExtensionsWithVersionedModels:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"allow_jinja_file_extensions": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_sql_v1.sql": versioned_model_sql_v1,
            "model_sql_v2.sql": versioned_model_sql_v2,
            "j2_suffixed_model_sql_v1.sql.j2": versioned_j2_model_sql_v1,
            "j2_suffixed_model_sql_v2.sql.j2": versioned_j2_model_sql_v2,
            "schema.yml": schema_versioned_yml,
        }

    def test_jinja_extensions_picked_up_by_parse(self, project):
        manifest = run_dbt(["parse"], expect_pass=True)

        expected_nodes = {
            "model.test.model_sql.v1",
            "model.test.model_sql.v2",
            "model.test.j2_suffixed_model_sql.v1",
            "model.test.j2_suffixed_model_sql.v2",
        }
        expected_node_name_counts = Counter({"model_sql": 2, "j2_suffixed_model_sql": 2})
        actual_node_name_counts = Counter(node.name for node in manifest.nodes.values())

        assert expected_nodes.issubset(set(manifest.nodes.keys()))
        assert expected_node_name_counts == actual_node_name_counts
