import json

from test.integration.base import DBTIntegrationTest, use_profile


class TestColumnComment(DBTIntegrationTest):
    @property
    def schema(self):
        return "column_comment_060"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    'materialized': 'table',
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def run_has_comments(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 1
        column_node = catalog_data['nodes']['model.test.quote_model']
        column_comment = column_node['columns']['2id']['comment']
        assert column_comment.startswith('XXX')

    @use_profile('postgres')
    def test_postgres_comments(self):
        self.run_has_comments()
