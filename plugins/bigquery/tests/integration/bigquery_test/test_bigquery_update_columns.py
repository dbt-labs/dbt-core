import os

from tests.integration.base import DBTIntegrationTest, use_profile


class TestBigqueryUpdateColumnPolicyTag(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "update-column-policy-tag"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'policy_tag': self.policy_tag
            }
        }

    @property
    def policy_tag(self):
        return os.environ.get('BIGQUERY_POLICY_TAG')

    @use_profile('bigquery')
    def test__bigquery_update_column_policy_tag(self):
        if self.policy_tag:
            results = self.run_dbt(['run', '--models', 'policy_tag_table'])
            self.assertEqual(len(results), 1)

            with self.get_connection() as conn:
                client = conn.handle

                table = client.get_table(
                    self.adapter.connections.get_bq_table(
                        self.default_database, self.unique_schema(), 'policy_tag_table')
                )

                for schema_field in table.schema:
                    self.assertEquals(schema_field.policy_tags.names,
                                      (self.policy_tag,))


class TestBigqueryUpdateColumnDescription(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "update-column-description"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'field_description': self.field_description
            }
        }

    @property
    def field_description(self):
        return 'this is a field'

    @use_profile('bigquery')
    def test__bigquery_update_column_description(self):
        results = self.run_dbt(['run', '--models', 'description_table'])
        self.assertEqual(len(results), 1)

        with self.get_connection() as conn:
            client = conn.handle

            table = client.get_table(
                self.adapter.connections.get_bq_table(
                    self.default_database, self.unique_schema(), 'description_table')
            )

            for schema_field in table.schema:
                self.assertEquals(schema_field.description, self.field_description)
