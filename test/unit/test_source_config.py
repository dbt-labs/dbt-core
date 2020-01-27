import os
from unittest import TestCase, mock

import dbt.flags
from dbt.node_types import NodeType
from dbt.source_config import SourceConfig

from .utils import config_from_parts_or_dicts


class SourceConfigTest(TestCase):
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
        self.patcher = mock.patch('dbt.context.parser.get_adapter')
        self.factory = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test__source_config_single_call(self):
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)
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
        self.assertEqual(cfg.config, expect)

    def test__source_config_multiple_calls(self):
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)
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
        self.assertEqual(cfg.config, expect)

    def test__source_config_merge(self):
        self.root_project_config.models = {'sort': ['a', 'b']}
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)
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
        self.assertEqual(cfg.config, expect)

    def test_source_config_all_keys_accounted_for(self):
        used_keys = frozenset(SourceConfig.AppendListFields) | \
                    frozenset(SourceConfig.ExtendDictFields) | \
                    frozenset(SourceConfig.ClobberFields)

        self.assertEqual(used_keys, frozenset(SourceConfig.ConfigKeys))

    def test__source_config_wrong_type(self):
        # ExtendDict fields should handle non-dict inputs gracefully
        self.root_project_config.models = {'persist_docs': False}
        cfg = SourceConfig(self.root_project_config, self.root_project_config,
                           ['root', 'x'], NodeType.Model)

        with self.assertRaises(dbt.exceptions.CompilationException) as exc:
            cfg.get_project_config(self.root_project_config)

        self.assertIn('must be a dict', str(exc.exception))
