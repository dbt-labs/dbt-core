from mock import MagicMock, patch
import os
import six
import unittest

import dbt.clients.system
import dbt.compilation
import dbt.exceptions
import dbt.flags
import dbt.linker
import dbt.config
import dbt.utils
import dbt.loader

try:
    from queue import Empty
except KeyError:
    from Queue import Empty


import networkx as nx

from dbt.logger import GLOBAL_LOGGER as logger # noqa

from .utils import config_from_parts_or_dicts


class GraphTest(unittest.TestCase):

    def tearDown(self):
        self.write_gpickle_patcher.stop()
        self.load_projects_patcher.stop()
        self.find_matching_patcher.stop()
        self.load_file_contents_patcher.stop()

    def setUp(self):
        dbt.flags.STRICT_MODE = True
        self.graph_result = None

        self.write_gpickle_patcher = patch('networkx.write_gpickle')
        self.load_projects_patcher = patch('dbt.loader._load_projects')
        self.find_matching_patcher = patch('dbt.clients.system.find_matching')
        self.load_file_contents_patcher = patch('dbt.clients.system.load_file_contents')

        def mock_write_gpickle(graph, outfile):
            self.graph_result = graph
        self.mock_write_gpickle = self.write_gpickle_patcher.start()
        self.mock_write_gpickle.side_effect = mock_write_gpickle

        self.profile = {
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'threads': 4,
                    'host': 'database',
                    'port': 5432,
                    'user': 'root',
                    'pass': 'password',
                    'dbname': 'dbt',
                    'schema': 'dbt_test'
                }
            },
            'target': 'test'
        }

        self.mock_load_projects = self.load_projects_patcher.start()
        self.mock_load_projects.return_value = []

        self.mock_models = []
        self.mock_content = {}

        def mock_find_matching(root_path, relative_paths_to_search, file_pattern):
            if 'sql' not in file_pattern:
                return []

            to_return = []

            if 'models' in relative_paths_to_search:
                to_return = to_return + self.mock_models

            return to_return

        self.mock_find_matching = self.find_matching_patcher.start()
        self.mock_find_matching.side_effect = mock_find_matching

        def mock_load_file_contents(path):
            return self.mock_content[path]

        self.mock_load_file_contents = self.load_file_contents_patcher.start()
        self.mock_load_file_contents.side_effect = mock_load_file_contents

    def get_config(self, extra_cfg=None):
        if extra_cfg is None:
            extra_cfg = {}

        cfg = {
            'name': 'test_models_compile',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }
        cfg.update(extra_cfg)

        return config_from_parts_or_dicts(project=cfg, profile=self.profile)

    def get_compiler(self, project):
        return dbt.compilation.Compiler(project)

    def use_models(self, models):
        for k, v in models.items():
            path = os.path.abspath('models/{}.sql'.format(k))
            self.mock_models.append({
                'searched_path': 'models',
                'absolute_path': path,
                'relative_path': '{}.sql'.format(k)})
            self.mock_content[path] = v

    def test__single_model(self):
        self.use_models({
            'model_one': 'select * from events',
        })

        config = self.get_config()
        manifest = dbt.loader.GraphLoader.load_all(config)
        compiler = self.get_compiler(config)
        linker = compiler.compile(manifest)

        self.assertEquals(
            linker.nodes(),
            ['model.test_models_compile.model_one'])

        self.assertEquals(
            linker.edges(),
            [])

    def test__two_models_simple_ref(self):
        self.use_models({
            'model_one': 'select * from events',
            'model_two': "select * from {{ref('model_one')}}",
        })

        config = self.get_config()
        manifest = dbt.loader.GraphLoader.load_all(config)
        compiler = self.get_compiler(config)
        linker = compiler.compile(manifest)

        six.assertCountEqual(self,
                             linker.nodes(),
                             [
                                 'model.test_models_compile.model_one',
                                 'model.test_models_compile.model_two',
                             ])

        six.assertCountEqual(
            self,
            linker.edges(),
            [ ('model.test_models_compile.model_one','model.test_models_compile.model_two',) ])

    def test__model_materializations(self):
        self.use_models({
            'model_one': 'select * from events',
            'model_two': "select * from {{ref('model_one')}}",
            'model_three': "select * from events",
            'model_four': "select * from events",
        })

        cfg = {
            "models": {
                "materialized": "table",
                "test_models_compile": {
                    "model_one": {"materialized": "table"},
                    "model_two": {"materialized": "view"},
                    "model_three": {"materialized": "ephemeral"}
                }
            }
        }

        config = self.get_config(cfg)
        manifest = dbt.loader.GraphLoader.load_all(config)
        compiler = self.get_compiler(config)
        linker = compiler.compile(manifest)

        expected_materialization = {
            "model_one": "table",
            "model_two": "view",
            "model_three": "ephemeral",
            "model_four": "table"
        }

        nodes = linker.graph.node

        for model, expected in expected_materialization.items():
            key = 'model.test_models_compile.{}'.format(model)
            actual = manifest.nodes[key].get('config', {}) \
                                             .get('materialized')
            self.assertEquals(actual, expected)

    def test__model_incremental(self):
        self.use_models({
            'model_one': 'select * from events'
        })

        cfg = {
            "models": {
                "test_models_compile": {
                    "model_one": {
                        "materialized": "incremental",
                        "sql_where": "created_at",
                        "unique_key": "id"
                    },
                }
            }
        }

        config = self.get_config(cfg)
        manifest = dbt.loader.GraphLoader.load_all(config)
        compiler = self.get_compiler(config)
        linker = compiler.compile(manifest)

        node = 'model.test_models_compile.model_one'

        self.assertEqual(linker.nodes(), [node])
        self.assertEqual(linker.edges(), [])

        self.assertEqual(
            manifest.nodes[node].get('config', {}).get('materialized'),
            'incremental'
        )

    def test__dependency_list(self):
        self.use_models({
            'model_1': 'select * from events',
            'model_2': 'select * from {{ ref("model_1") }}',
            'model_3': '''
                select * from {{ ref("model_1") }}
                union all
                select * from {{ ref("model_2") }}
            ''',
            'model_4': 'select * from {{ ref("model_3") }}'
        })

        config = self.get_config()
        graph = dbt.loader.GraphLoader.load_all(config)
        compiler = self.get_compiler(config)
        linker = compiler.compile(graph)

        models = ('model_1', 'model_2', 'model_3', 'model_4')
        model_ids = ['model.test_models_compile.{}'.format(m) for m in models]

        manifest = MagicMock(nodes={
            n: MagicMock(unique_id=n)
            for n in model_ids
        })
        queue = linker.as_graph_queue(manifest)

        for model_id in model_ids:
            self.assertFalse(queue.empty())
            got = queue.get(block=False)
            self.assertEqual(got.unique_id, model_id)
            with self.assertRaises(Empty):
                queue.get(block=False)
            queue.mark_done(got.unique_id)
        self.assertTrue(queue.empty())
