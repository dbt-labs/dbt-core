import unittest

from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger


class TestPostgresAdapter(unittest.TestCase):

    def setUp(self):
        self.profile = {
            'dbname': 'postgres',
            'user': 'root',
            'host': 'database',
            'password': 'password',
            'port': 5432,
        }

    def test_acquire_connection_validations(self):
        cfg = { 'STRICT_MODE': True }

        try:
            connection = PostgresAdapter.acquire_connection(cfg, self.profile)
            self.assertEquals(connection.get('type'), 'postgres')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('validation failed with unknown exception: {}'
                      .format(str(e)))

    def test_acquire_connection(self):
        cfg = { 'STRICT_MODE': True }

        connection = PostgresAdapter.acquire_connection(cfg, self.profile)

        self.assertEquals(connection.get('state'), 'open')
        self.assertNotEquals(connection.get('handle'), None)
