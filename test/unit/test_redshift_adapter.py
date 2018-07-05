import unittest
import mock

import dbt.flags as flags
import dbt.utils

from dbt.adapters.redshift import RedshiftAdapter
from dbt.exceptions import ValidationException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger  # noqa

@classmethod
def fetch_cluster_credentials(*args, **kwargs):
    return {
        'DbUser': 'root',
        'DbPassword': 'tmp_password'
    }

class TestRedshiftAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

    def test_implicit_database_conn(self):
        implicit_database_profile = {
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5439,
            'schema': 'public'
        }

        creds = RedshiftAdapter.get_credentials(implicit_database_profile)
        self.assertEquals(creds, implicit_database_profile)

    def test_explicit_database_conn(self):
        explicit_database_profile = {
            'method': 'database',
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5439,
            'schema': 'public'
        }

        creds = RedshiftAdapter.get_credentials(explicit_database_profile)
        self.assertEquals(creds, explicit_database_profile)

    def test_explicit_iam_conn(self):
        explicit_iam_profile = {
            'method': 'iam',
            'cluster_id': 'my_redshift',
            'iam_duration_s': 1200,
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'port': 5439,
            'schema': 'public',
        }

        with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
            creds = RedshiftAdapter.get_credentials(explicit_iam_profile)

        expected_creds = dbt.utils.merge(explicit_iam_profile, {'pass': 'tmp_password'})
        self.assertEquals(creds, expected_creds)

    def test_invalid_auth_method(self):
        invalid_profile = {
            'method': 'badmethod',
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5439,
            'schema': 'public'
        }

        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.get_credentials(invalid_profile)

        self.assertTrue('badmethod' in context.exception.msg)

    def test_invalid_iam_no_cluster_id(self):
        invalid_profile = {
            'method': 'iam',
            'dbname': 'redshift',
            'user': 'root',
            'host': 'database',
            'port': 5439,
            'schema': 'public'
        }
        with self.assertRaises(dbt.exceptions.FailedToConnectException) as context:
            with mock.patch.object(RedshiftAdapter, 'fetch_cluster_credentials', new=fetch_cluster_credentials):
                RedshiftAdapter.get_credentials(invalid_profile)

        self.assertTrue("'cluster_id' must be provided" in context.exception.msg)
