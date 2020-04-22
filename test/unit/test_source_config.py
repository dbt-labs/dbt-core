import os
from unittest import TestCase, mock

from dbt.adapters import postgres  # we want this available!
import dbt.flags
from dbt.context.context_config import LegacyContextConfig
from dbt.legacy_config_updater import ConfigUpdater
from dbt.node_types import NodeType

from .utils import config_from_parts_or_dicts


class LegacyContextConfigTest(TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True
        dbt.flags.WARN_ERROR = True

        self.maxDiff = None

        profile_data = {
            'target': 'test',
            'quoting': {},
            'outputs': {
                'test': {
                    'type': 'redshift',
                    'host': 'localhost',
                    'schema': 'analytics',
                    'user': 'test',
                    'pass': 'test',
                    'dbname': 'test',
                    'port': 1,
                }
            }
        }

        root_project = {
            'name': 'root',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

        self.root_project_config = config_from_parts_or_dicts(
            project=root_project,
            profile=profile_data,
            cli_vars='{"test_schema_name": "foo"}'
        )

        snowplow_project = {
            'name': 'snowplow',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('./dbt_modules/snowplow'),
        }

        self.snowplow_project_config = config_from_parts_or_dicts(
            project=snowplow_project, profile=profile_data
        )

        self.all_projects = {
            'root': self.root_project_config,
            'snowplow': self.snowplow_project_config
        }
        self.patcher = mock.patch('dbt.context.providers.get_adapter')
        self.factory = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test__context_config_single_call(self):
        cfg = LegacyContextConfig(
            self.root_project_config, self.root_project_config,
            ['root', 'x'], NodeType.Model
        )
        cfg.update_in_model_config({
            'materialized': 'something',
            'sort': 'my sort key',
            'pre-hook': 'my pre run hook',
            'vars': {'a': 1, 'b': 2},
        })
        expect = {
            'column_types': {},
            'enabled': True,
            'materialized': 'something',
            'post-hook': [],
            'pre-hook': ['my pre run hook'],
            'persist_docs': {},
            'quoting': {},
            'sort': 'my sort key',
            'tags': [],
            'vars': {'a': 1, 'b': 2},
        }
        self.assertEqual(cfg.build_config_dict(), expect)

    def test__context_config_multiple_calls(self):
        cfg = LegacyContextConfig(
            self.root_project_config, self.root_project_config,
            ['root', 'x'], NodeType.Model
        )
        cfg.update_in_model_config({
            'materialized': 'something',
            'sort': 'my sort key',
            'pre-hook': 'my pre run hook',
            'vars': {'a': 1, 'b': 2},
        })
        cfg.update_in_model_config({
            'materialized': 'something else',
            'pre-hook': ['my other pre run hook', 'another pre run hook'],
            'vars': {'a': 4, 'c': 3},
        })
        expect = {
            'column_types': {},
            'enabled': True,
            'materialized': 'something else',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [
                'my pre run hook',
                'my other pre run hook',
                'another pre run hook',
            ],
            'quoting': {},
            'sort': 'my sort key',
            'tags': [],
            'vars': {'a': 4, 'b': 2, 'c': 3},
        }
        self.assertEqual(cfg.build_config_dict(), expect)

    def test__context_config_merge(self):
        self.root_project_config.models = {'sort': ['a', 'b']}
        cfg = LegacyContextConfig(
            self.root_project_config, self.root_project_config,
            ['root', 'x'], NodeType.Model
        )
        cfg.update_in_model_config({
            'materialized': 'something',
            'sort': ['d', 'e']
        })
        expect = {
            'column_types': {},
            'enabled': True,
            'materialized': 'something',
            'post-hook': [],
            'pre-hook': [],
            'persist_docs': {},
            'quoting': {},
            'sort': ['d', 'e'],
            'tags': [],
            'vars': {},
        }
        self.assertEqual(cfg.build_config_dict(), expect)

    def test_context_config_all_keys_accounted_for(self):
        updater = ConfigUpdater('postgres')
        used_keys = (
            frozenset(updater.AppendListFields) |
            frozenset(updater.ExtendDictFields) |
            frozenset(updater.ClobberFields) |
            frozenset({'unlogged'})
        )

        self.assertEqual(used_keys, frozenset(updater.ConfigKeys))

    def test__context_config_wrong_type(self):
        # ExtendDict fields should handle non-dict inputs gracefully
        self.root_project_config.models = {'persist_docs': False}
        cfg = LegacyContextConfig(
            self.root_project_config, self.root_project_config,
            ['root', 'x'], NodeType.Model
        )

        model = mock.MagicMock(resource_type=NodeType.Model, fqn=['root', 'x'], project_name='root')

        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            cfg.updater.get_project_config(model, self.root_project_config)

        self.assertIn('must be a dict', str(exc.exception))
