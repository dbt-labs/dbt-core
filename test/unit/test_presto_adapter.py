import mock
import unittest
import dbt.adapters
import dbt.flags as flags
from dbt.adapters.presto import PrestoAdapter
import agate

from .utils import config_from_parts_or_dicts, inject_adapter

class TestPrestoAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'presto',
                    'catalog': 'prestodb',
                    'host': 'database',
                    'port': 5439,
                    'schema': 'dbt_test_schema',
                    'method': 'none',
                }
            },
            'target': 'test'
        }

        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            }
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = PrestoAdapter(self.config)
        return self._adapter

    def test_acquire_connection(self):
        connection = self.adapter.acquire_connection('dummy')

        self.assertEquals(connection.state, 'open')
        self.assertNotEquals(connection.handle, None)

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        self.adapter.connections.in_use['master'] = mock.MagicMock()
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)
