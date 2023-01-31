import json
import os
import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.persist_docs.fixtures import (
    _DOCS__MY_FUN_DOCS,
    _MODELS__MISSING_COLUMN,
    _MODELS__MODEL_USING_QUOTE_UTIL,
    _MODELS__NO_DOCS_MODEL,
    _MODELS__TABLE,
    _MODELS__VIEW,
    _PROPERTIES__QUOTE_MODEL,
    _PROPERITES__SCHEMA_MISSING_COL,
    _PROPERTIES__SCHEMA_YML,
    _SEEDS__SEED,
)


class BasePersistDocsBase:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        run_dbt(["seed"])
        run_dbt()

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_docs_model.sql": _MODELS__NO_DOCS_MODEL,
            "table_model.sql": _MODELS__TABLE,
            "view_model.sql": _MODELS__VIEW,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {
            "my_fun_docs.md": _DOCS__MY_FUN_DOCS,
            "schema.yml": _PROPERTIES__SCHEMA_YML,
        }

    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert '"with double quotes"' in comment
            assert """'''abc123'''""" in comment
            assert "\n" in comment
            assert "Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting" in comment
            assert "/* comment */" in comment
            if os.name == "nt":
                assert "--\r\n" in comment or "--\n" in comment
            else:
                assert "--\n" in comment

    def _assert_has_table_comments(self, table_node):
        table_comment = table_node["metadata"]["comment"]
        assert table_comment.startswith("Table model description")

        table_id_comment = table_node["columns"]["id"]["comment"]
        assert table_id_comment.startswith("id Column description")

        table_name_comment = table_node["columns"]["name"]["comment"]
        assert table_name_comment.startswith("Some stuff here and then a call to")

        self._assert_common_comments(table_comment, table_id_comment, table_name_comment)

    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_comment = view_node["metadata"]["comment"]
        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert view_comment is None

        view_id_comment = view_node["columns"]["id"]["comment"]
        if has_column_comments:
            assert view_id_comment.startswith("id Column description")
            self._assert_common_comments(view_id_comment)
        else:
            assert view_id_comment is None

        view_name_comment = view_node["columns"]["name"]["comment"]
        assert view_name_comment is None


class BasePersistDocs(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_has_comments_pglike(self, project):
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 4
        table_node = catalog_data["nodes"]["model.test.table_model"]
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data["nodes"]["model.test.view_model"]
        self._assert_has_view_comments(view_node)

        no_docs_node = catalog_data["nodes"]["model.test.no_docs_model"]
        self._assert_has_view_comments(no_docs_node, False, False)


class BasePersistDocsColumnMissing(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column.sql": _MODELS__MISSING_COLUMN}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _PROPERITES__SCHEMA_MISSING_COL}

    def test_missing_column(self, project):
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data

        table_node = catalog_data["nodes"]["model.test.missing_column"]
        table_id_comment = table_node["columns"]["id"]["comment"]
        assert table_id_comment.startswith("test id column description")


class BasePersistDocsCommentOnQuotedColumn:
    """Covers edge case where column with comment must be quoted.
    We set this using the `quote:` tag in the property file."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"quote_model.sql": _MODELS__MODEL_USING_QUOTE_UTIL}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"properties.yml": _PROPERTIES__QUOTE_MODEL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def run_has_comments(self, project):
        def fixt():
            run_dbt()
            run_dbt(["docs", "generate"])
            with open("target/catalog.json") as fp:
                catalog_data = json.load(fp)
            assert "nodes" in catalog_data
            assert len(catalog_data["nodes"]) == 1
            column_node = catalog_data["nodes"]["model.test.quote_model"]
            column_comment = column_node["columns"]["2id"]["comment"]
            assert column_comment.startswith("XXX")

        return fixt

    def test_quoted_column_comments(self, run_has_comments):
        run_has_comments()


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
