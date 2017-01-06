import unittest

import dbt.flags as flags

from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger


class TestPostgresAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        self.profile = {
            'dbname': 'postgres',
            'user': 'root',
            'host': 'database',
            'pass': 'password',
            'port': 5432,
            'schema': 'public'
        }

    def test_acquire_connection_validations(self):
        try:
            connection = PostgresAdapter.acquire_connection(self.profile)
            self.assertEquals(connection.get('type'), 'postgres')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('validation failed with unknown exception: {}'
                      .format(str(e)))

    def test_acquire_connection(self):
        connection = PostgresAdapter.acquire_connection(self.profile)

        self.assertEquals(connection.get('state'), 'open')
        self.assertNotEquals(connection.get('handle'), None)

    def test__get_connection(self):
        connection = PostgresAdapter.get_connection(self.profile)
        duplicate = PostgresAdapter.get_connection(self.profile)

        self.assertEquals(connection.get('state'), 'open')
        self.assertNotEquals(connection.get('handle'), None)

        self.assertEquals(connection, duplicate)
