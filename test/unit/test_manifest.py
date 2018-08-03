import unittest

import copy
import os

import dbt.flags
from dbt.contracts.graph.parsed import ParsedNode, ParsedManifest

class ManifestTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
        }

        self.nested_nodes = {
            'model.snowplow.events': ParsedNode(
                name='events',
                schema='analytics',
                alias='events',
                resource_type='model',
                unique_id='model.snowplow.events',
                fqn=['snowplow', 'events'],
                empty=False,
                package_name='snowplow',
                refs=[],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.events': ParsedNode(
                name='events',
                schema='analytics',
                alias='events',
                resource_type='model',
                unique_id='model.root.events',
                fqn=['root', 'events'],
                empty=False,
                package_name='root',
                refs=[],
                depends_on={
                    'nodes': [],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.dep': ParsedNode(
                name='dep',
                schema='analytics',
                alias='dep',
                resource_type='model',
                unique_id='model.root.dep',
                fqn=['root', 'dep'],
                empty=False,
                package_name='root',
                refs=[['events']],
                depends_on={
                    'nodes': ['model.root.events'],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.nested': ParsedNode(
                name='nested',
                schema='analytics',
                alias='nested',
                resource_type='model',
                unique_id='model.root.nested',
                fqn=['root', 'nested'],
                empty=False,
                package_name='root',
                refs=[['events']],
                depends_on={
                    'nodes': ['model.root.dep'],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.sibling': ParsedNode(
                name='sibling',
                schema='analytics',
                alias='sibling',
                resource_type='model',
                unique_id='model.root.sibling',
                fqn=['root', 'sibling'],
                empty=False,
                package_name='root',
                refs=[['events']],
                depends_on={
                    'nodes': ['model.root.events'],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
            'model.root.multi': ParsedNode(
                name='multi',
                schema='analytics',
                alias='multi',
                resource_type='model',
                unique_id='model.root.multi',
                fqn=['root', 'multi'],
                empty=False,
                package_name='root',
                refs=[['events']],
                depends_on={
                    'nodes': ['model.root.nested', 'model.root.sibling'],
                    'macros': []
                },
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                raw_sql='does not matter'
            ),
        }

    def test__no_nodes(self):
        manifest = ParsedManifest(nodes={}, macros={})
        self.assertEqual(
            manifest.serialize(),
            {'nodes': {}, 'macros': {}, 'parent_map': {}, 'child_map': {}}
        )

    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = ParsedManifest(nodes=nodes, macros={})
        serialized = manifest.serialize()
        parent_map = serialized['parent_map']
        child_map = serialized['child_map']
        # make sure there aren't any extra/missing keys.
        self.assertEqual(set(parent_map), set(nodes))
        self.assertEqual(set(child_map), set(nodes))
        self.assertEqual(
            parent_map['model.root.sibling'],
            ['model.root.events']
        )
        self.assertEqual(
            parent_map['model.root.nested'],
            ['model.root.dep']
        )
        self.assertEqual(
            parent_map['model.root.dep'],
            ['model.root.events']
        )
        # order doesn't matter.
        self.assertEqual(
            set(parent_map['model.root.multi']),
            set(['model.root.nested', 'model.root.sibling'])
        )
        self.assertEqual(
            parent_map['model.root.events'],
            [],
        )
        self.assertEqual(
            parent_map['model.snowplow.events'],
            [],
        )

        self.assertEqual(
            child_map['model.root.sibling'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.nested'],
            ['model.root.multi'],
        )
        self.assertEqual(
            child_map['model.root.dep'],
            ['model.root.nested']
        )
        self.assertEqual(
            child_map['model.root.multi'],
            []
        )
        self.assertEqual(
            set(child_map['model.root.events']),
            set(['model.root.dep', 'model.root.sibling'])
        )
        self.assertEqual(
            child_map['model.snowplow.events'],
            []
        )

    def test__to_flat_graph(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = ParsedManifest(nodes=nodes, macros={})
        flat_graph = manifest.to_flat_graph()
        flat_nodes = flat_graph['nodes']
        self.assertEqual(set(flat_graph), set(['nodes', 'macros']))
        self.assertEqual(flat_graph['macros'], {})
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        expected_keys = set(ParsedNode.SCHEMA['required']) | {'agate_table'}
        for node in flat_nodes.values():
            self.assertEqual(set(node), expected_keys)
