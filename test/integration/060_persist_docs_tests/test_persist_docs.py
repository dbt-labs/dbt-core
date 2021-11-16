from test.integration.base import DBTIntegrationTest, use_profile
import os

import json


class BasePersistDocsTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "persist_docs_060"

    @property
    def models(self):
        return "models"

    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert '"with double quotes"' in comment
            assert """'''abc123'''""" in comment
            assert '\n' in comment
            assert 'Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting' in comment
            assert '/* comment */' in comment
            if os.name == 'nt':
                assert '--\r\n' in comment or '--\n' in comment
            else:
                assert '--\n' in comment

    def _assert_has_table_comments(self, table_node):
        table_comment = table_node['metadata']['comment']
        assert table_comment.startswith('Table model description')

        table_id_comment = table_node['columns']['id']['comment']
        assert table_id_comment.startswith('id Column description')

        table_name_comment = table_node['columns']['name']['comment']
        assert table_name_comment.startswith(
            'Some stuff here and then a call to')

        self._assert_common_comments(
            table_comment, table_id_comment, table_name_comment
        )

    def _assert_has_view_comments(self, view_node, has_node_comments=True,
                                  has_column_comments=True):
        view_comment = view_node['metadata']['comment']
        if has_node_comments:
            assert view_comment.startswith('View model description')
            self._assert_common_comments(view_comment)
        else:
            assert view_comment is None

        view_id_comment = view_node['columns']['id']['comment']
        if has_column_comments:
            assert view_id_comment.startswith('id Column description')
            self._assert_common_comments(view_id_comment)
        else:
            assert view_id_comment is None

        view_name_comment = view_node['columns']['name']['comment']
        assert view_name_comment is None


class TestPersistDocs(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def run_has_comments_pglike(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 3
        table_node = catalog_data['nodes']['model.test.table_model']
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data['nodes']['model.test.view_model']
        self._assert_has_view_comments(view_node)

        no_docs_node = catalog_data['nodes']['model.test.no_docs_model']
        self._assert_has_view_comments(no_docs_node, False, False)

    @use_profile('postgres')
    def test_postgres_comments(self):
        self.run_has_comments_pglike()

class TestPersistDocsColumnMissing(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "columns": True,
                    },
                }
            }
        }

    @property
    def models(self):
        return 'models-column-missing'

    @use_profile('postgres')
    def test_postgres_missing_column(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data

        table_node = catalog_data['nodes']['model.test.missing_column']
        table_id_comment = table_node['columns']['id']['comment']
        assert table_id_comment.startswith('test id column description')
