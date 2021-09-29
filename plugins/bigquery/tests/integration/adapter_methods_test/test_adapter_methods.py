from tests.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestBaseCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "adapter_methods_caching"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'test-paths': ['tests']
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_dbt()
        self.assertTablesEqual('model', 'expected')


class TestRenameRelation(DBTIntegrationTest):
    @property
    def schema(self):
        return "adapter_methods_rename_relation"

    @property
    def models(self):
        return 'bigquery-models'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'source-paths': ['models']
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_methods(self):
        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_sql_file("seed_bq.sql")
        self.run_dbt(['seed'])
        rename_relation_args = yaml.safe_dump({
            'from_name': 'seed',
            'to_name': 'renamed_seed',
        })
        self.run_dbt(['run-operation', 'rename_named_relation', '--args', rename_relation_args])
        self.run_dbt()


class TestGrantAccess(DBTIntegrationTest):
    @property
    def schema(self):
        return "adapter_methods_grant_access"

    @property
    def models(self):
        return 'bigquery-models'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'source-paths': ['models']
        }

    @use_profile('bigquery')
    def test_bigquery_adapter_methods(self):
        from dbt.adapters.bigquery import GrantTarget
        from google.cloud.bigquery import AccessEntry

        self.run_dbt(['compile'])  # trigger any compile-time issues
        self.run_sql_file("seed_bq.sql")
        self.run_dbt(['seed'])

        ae_role = "READER"
        ae_entity = "user@email.com"
        ae_entity_type = "userByEmail"
        ae_grant_target_dict = {
            'project': self.default_database,
            'dataset': self.unique_schema()
        }
        self.adapter.grant_access_to(ae_entity, ae_entity_type, ae_role, ae_grant_target_dict)

        conn = self.adapter.connections.get_thread_connection()
        client = conn.handle

        grant_target = GrantTarget.from_dict(ae_grant_target_dict)
        dataset = client.get_dataset(
            self.adapter.connections.dataset_from_id(grant_target.render())
        )

        expected_access_entry = AccessEntry(ae_role, ae_entity_type, ae_entity)
        self.assertTrue(expected_access_entry in dataset.access_entries)

        unexpected_access_entry = AccessEntry(ae_role, ae_entity_type, "unexpected@email.com")
        self.assertFalse(unexpected_access_entry in dataset.access_entries)
