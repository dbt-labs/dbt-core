from test.integration.base import DBTIntegrationTest, use_profile
import pickle
import os


class TestRpcExecuteReturnsResults(DBTIntegrationTest):

    @property
    def schema(self):
        return "rpc_test_048"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
        }

    def do_test_pickle(self, agate_table):
        table = {
            'column_names': list(agate_table.column_names),
            'rows': [list(row) for row in agate_table]
        }

        pickle.dumps(table)

    def do_test_file(self, filename):
        file_path = os.path.join("sql", filename)
        with open(file_path) as fh:
            query = fh.read()

        status, table = self.adapter.execute(query, auto_begin=False, fetch=True)
        self.assertTrue(len(table.columns) > 0, "agate table had no columns")
        self.assertTrue(len(table.rows) > 0, "agate table had no rows")

        self.do_test_pickle(table)

    @use_profile('bigquery')
    def test__bigquery_fetch_and_serialize(self):
        self.do_test_file('bigquery.sql')

    @use_profile('snowflake')
    def test__snowflake_fetch_and_serialize(self):
        self.do_test_file('snowflake.sql')

    @use_profile('redshift')
    def test__redshift_fetch_and_serialize(self):
        self.do_test_file('redshift.sql')
