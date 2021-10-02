from test.integration.base import DBTIntegrationTest, use_profile
import pickle
import os


class TestRpcExecuteReturnsResults(DBTIntegrationTest):

    @property
    def schema(self):
        return "rpc_test_100"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'source-paths': ['rpc_serialize_test_models'],
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

        with self.adapter.connection_named('master'):
            status, table = self.adapter.execute(query, auto_begin=False, fetch=True)
        self.assertTrue(len(table.columns) > 0, "agate table had no columns")
        self.assertTrue(len(table.rows) > 0, "agate table had no rows")

        self.do_test_pickle(table)
        return table

    def assert_all_columns_are_strings(self, table):
        for row in table:
            for value in row:
                self.assertEqual(type(value), str, f'Found a not-string: {value} in row {row}')

    # commenting these out for now, to avoid raising undefined profiles error

    #@use_profile('bigquery')
    def test__bigquery_fetch_and_serialize(self):
        self.do_test_file('bigquery.sql')

    #@use_profile('snowflake')
    def test__snowflake_fetch_and_serialize(self):
        self.do_test_file('snowflake.sql')

    #@use_profile('redshift')
    def test__redshift_fetch_and_serialize(self):
        self.do_test_file('redshift.sql')

    #@use_profile('bigquery')
    def test__bigquery_type_coercion(self):
        table = self.do_test_file('generic.sql')
        self.assert_all_columns_are_strings(table)

    #@use_profile('snowflake')
    def test__snowflake_type_coercion(self):
        table = self.do_test_file('generic.sql')
        self.assert_all_columns_are_strings(table)

    #@use_profile('redshift')
    def test__redshift_type_coercion(self):
        table = self.do_test_file('generic.sql')
        self.assert_all_columns_are_strings(table)
