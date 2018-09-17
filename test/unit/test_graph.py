from mock import MagicMock
import os
import six
import unittest

import dbt.clients.system
import dbt.compilation
import dbt.exceptions
import dbt.flags
import dbt.linker
import dbt.model
import dbt.config
import dbt.templates
import dbt.utils

import networkx as nx

from dbt.logger import GLOBAL_LOGGER as logger # noqa

from .utils import config_from_parts_or_dicts


class GraphTest(unittest.TestCase):

    def tearDown(self):
        nx.write_gpickle = self.real_write_gpickle
        dbt.utils.dependency_projects = self.real_dependency_projects
        dbt.clients.system.find_matching = self.real_find_matching
        dbt.clients.system.load_file_contents = self.real_load_file_contents

    def setUp(self):
        dbt.flags.STRICT_MODE = True

        def mock_write_gpickle(graph, outfile):
            self.graph_result = graph

        self.real_write_gpickle = nx.write_gpickle
        nx.write_gpickle = mock_write_gpickle

        self.graph_result = None

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

        self.real_dependency_projects = dbt.utils.dependency_projects
        dbt.utils.dependency_projects = MagicMock(return_value=[])

        self.mock_models = []
        self.mock_content = {}

        def mock_find_matching(root_path, relative_paths_to_search,
                               file_pattern):
            if 'sql' not in file_pattern:
                return []

            to_return = []

            if 'models' in relative_paths_to_search:
                to_return = to_return + self.mock_models

            return to_return

        self.real_find_matching = dbt.clients.system.find_matching
        dbt.clients.system.find_matching = MagicMock(
            side_effect=mock_find_matching)

        def mock_load_file_contents(path):
            return self.mock_content[path]

        self.real_load_file_contents = dbt.clients.system.load_file_contents
        dbt.clients.system.load_file_contents = MagicMock(
            side_effect=mock_load_file_contents)

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

        compiler = self.get_compiler(self.get_config())
        graph, linker = compiler.compile()

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

        compiler = self.get_compiler(self.get_config())
        graph, linker = compiler.compile()

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

        compiler = self.get_compiler(self.get_config(cfg))
        graph, linker = compiler.compile()

        expected_materialization = {
            "model_one": "table",
            "model_two": "view",
            "model_three": "ephemeral",
            "model_four": "table"
        }

        nodes = linker.graph.node

        for model, expected in expected_materialization.items():
            key = 'model.test_models_compile.{}'.format(model)
            actual = nodes[key].get('config', {}) \
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

        compiler = self.get_compiler(self.get_config(cfg))
        graph, linker = compiler.compile()

        node = 'model.test_models_compile.model_one'

        self.assertEqual(linker.nodes(), [node])
        self.assertEqual(linker.edges(), [])

        self.assertEqual(
                linker.graph.node[node].get('config', {}).get('materialized'),
                'incremental')

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

        compiler = self.get_compiler(self.get_config({}))
        graph, linker = compiler.compile()

        actual_dep_list = linker.as_dependency_list()

        expected_dep_list = [
            ['model.test_models_compile.model_1'],
            ['model.test_models_compile.model_2'],
            ['model.test_models_compile.model_3'],
            ['model.test_models_compile.model_4'],
        ]

        self.assertEqual(actual_dep_list, expected_dep_list)
