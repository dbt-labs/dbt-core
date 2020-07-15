import unittest
from unittest import mock

import copy
from collections import namedtuple
from itertools import product
from datetime import datetime

import pytest

import dbt.flags
from dbt import tracking
from dbt.contracts.graph.manifest import Manifest, ManifestMetadata
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
    DependsOn,
    NodeConfig,
    ParsedSeedNode,
    ParsedSourceDefinition,
)
from dbt.contracts.graph.compiled import CompiledModelNode
from dbt.node_types import NodeType
import freezegun

from .utils import MockMacro, MockDocumentation, MockSource, MockNode, MockMaterialization, MockGenerateMacro


REQUIRED_PARSED_NODE_KEYS = frozenset({
    'alias', 'tags', 'config', 'unique_id', 'refs', 'sources', 'meta',
    'depends_on', 'database', 'schema', 'name', 'resource_type',
    'package_name', 'root_path', 'path', 'original_file_path', 'raw_sql',
    'description', 'columns', 'fqn', 'build_path', 'patch_path', 'docs',
})

REQUIRED_COMPILED_NODE_KEYS = frozenset(REQUIRED_PARSED_NODE_KEYS | {
    'compiled', 'extra_ctes_injected', 'extra_ctes', 'compiled_sql',
    'injected_sql',
})


class ManifestTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        })

        self.nested_nodes = {
            'model.snowplow.events': ParsedModelNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.snowplow.events',
                fqn=['snowplow', 'events'],
                package_name='snowplow',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.events': ParsedModelNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.root.events',
                fqn=['root', 'events'],
                package_name='root',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.dep': ParsedModelNode(
                name='dep',
                database='dbt',
                schema='analytics',
                alias='dep',
                resource_type=NodeType.Model,
                unique_id='model.root.dep',
                fqn=['root', 'dep'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.nested': ParsedModelNode(
                name='nested',
                database='dbt',
                schema='analytics',
                alias='nested',
                resource_type=NodeType.Model,
                unique_id='model.root.nested',
                fqn=['root', 'nested'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.dep']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.sibling': ParsedModelNode(
                name='sibling',
                database='dbt',
                schema='analytics',
                alias='sibling',
                resource_type=NodeType.Model,
                unique_id='model.root.sibling',
                fqn=['root', 'sibling'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.multi': ParsedModelNode(
                name='multi',
                database='dbt',
                schema='analytics',
                alias='multi',
                resource_type=NodeType.Model,
                unique_id='model.root.multi',
                fqn=['root', 'multi'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.nested', 'model.root.sibling']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
        }

        self.sources = {
            'source.root.my_source.my_table': ParsedSourceDefinition(
                database='raw',
                schema='analytics',
                resource_type=NodeType.Source,
                identifier='some_source',
                name='my_table',
                source_name='my_source',
                source_description='My source description',
                description='Table description',
                loader='a_loader',
                unique_id='source.test.my_source.my_table',
                fqn=['test', 'my_source', 'my_table'],
                package_name='root',
                root_path='',
                path='schema.yml',
                original_file_path='schema.yml',
            ),
        }
        for node in self.nested_nodes.values():
            node.validate(node.to_dict())
        for source in self.sources.values():
            source.validate(source.to_dict())

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'sources': {},
                'macros': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {},
                'disabled': [],
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        serialized = manifest.writable_manifest().to_dict()
        self.assertEqual(serialized['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['docs'], {})
        self.assertEqual(serialized['disabled'], [])
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

    def test__build_flat_graph(self):
        nodes = copy.copy(self.nested_nodes)
        sources = copy.copy(self.sources)
        manifest = Manifest(nodes=nodes, sources=sources, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        manifest.build_flat_graph()
        flat_graph = manifest.flat_graph
        flat_nodes = flat_graph['nodes']
        flat_sources = flat_graph['sources']
        self.assertEqual(set(flat_graph), set(['nodes', 'sources']))
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        self.assertEqual(set(flat_sources), set(self.sources))
        for node in flat_nodes.values():
            self.assertEqual(frozenset(node), REQUIRED_PARSED_NODE_KEYS)

    @mock.patch.object(tracking, 'active_user')
    def test_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        mock_user.do_not_track = True
        self.assertEqual(
            ManifestMetadata(
                project_id='098f6bcd4621d373cade4e832627b4f6',
                adapter_type='postgres',
            ),
            ManifestMetadata(
                project_id='098f6bcd4621d373cade4e832627b4f6',
                user_id='cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                send_anonymous_usage_stats=False,
                adapter_type='postgres',
            )
        )

    @mock.patch.object(tracking, 'active_user')
    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test_no_nodes_with_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        mock_user.do_not_track = True
        metadata = ManifestMetadata(
            project_id='098f6bcd4621d373cade4e832627b4f6',
            adapter_type='postgres',
        )
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            metadata=metadata, files={})

        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'sources': {},
                'macros': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {
                    'project_id': '098f6bcd4621d373cade4e832627b4f6',
                    'user_id': 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                    'send_anonymous_usage_stats': False,
                    'adapter_type': 'postgres',
                },
                'disabled': [],
            }
        )

    def test_get_resource_fqns_empty(self):
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        self.assertEqual(manifest.get_resource_fqns(), {})

    def test_get_resource_fqns(self):
        nodes = copy.copy(self.nested_nodes)
        nodes['seed.root.seed'] = ParsedSeedNode(
            name='seed',
            database='dbt',
            schema='analytics',
            alias='seed',
            resource_type=NodeType.Seed,
            unique_id='seed.root.seed',
            fqn=['root', 'seed'],
            package_name='root',
            refs=[['events']],
            sources=[],
            depends_on=DependsOn(),
            config=self.model_config,
            tags=[],
            path='seed.csv',
            original_file_path='seed.csv',
            root_path='',
            raw_sql='-- csv --',
        )
        manifest = Manifest(nodes=nodes, sources=self.sources, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        expect = {
            'models': frozenset([
                ('snowplow', 'events'),
                ('root', 'events'),
                ('root', 'dep'),
                ('root', 'nested'),
                ('root', 'sibling'),
                ('root', 'multi'),
            ]),
            'seeds': frozenset([
                ('root', 'seed')
            ]),
            'sources': frozenset([
                ('test', 'my_source', 'my_table')
            ])
        }
        resource_fqns = manifest.get_resource_fqns()
        self.assertEqual(resource_fqns, expect)


class MixedManifestTest(unittest.TestCase):
    def setUp(self):
        dbt.flags.STRICT_MODE = True

        self.maxDiff = None

        self.model_config = NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        })

        self.nested_nodes = {
            'model.snowplow.events': CompiledModelNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.snowplow.events',
                fqn=['snowplow', 'events'],
                package_name='snowplow',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter',
                meta={},
                compiled=True,
                compiled_sql='also does not matter',
                extra_ctes_injected=True,
                injected_sql=None,
                extra_ctes=[]
            ),
            'model.root.events': CompiledModelNode(
                name='events',
                database='dbt',
                schema='analytics',
                alias='events',
                resource_type=NodeType.Model,
                unique_id='model.root.events',
                fqn=['root', 'events'],
                package_name='root',
                refs=[],
                sources=[],
                depends_on=DependsOn(),
                config=self.model_config,
                tags=[],
                path='events.sql',
                original_file_path='events.sql',
                root_path='',
                raw_sql='does not matter',
                meta={},
                compiled=True,
                compiled_sql='also does not matter',
                extra_ctes_injected=True,
                injected_sql='and this also does not matter',
                extra_ctes=[]
            ),
            'model.root.dep': ParsedModelNode(
                name='dep',
                database='dbt',
                schema='analytics',
                alias='dep',
                resource_type=NodeType.Model,
                unique_id='model.root.dep',
                fqn=['root', 'dep'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.nested': ParsedModelNode(
                name='nested',
                database='dbt',
                schema='analytics',
                alias='nested',
                resource_type=NodeType.Model,
                unique_id='model.root.nested',
                fqn=['root', 'nested'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.dep']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.sibling': ParsedModelNode(
                name='sibling',
                database='dbt',
                schema='analytics',
                alias='sibling',
                resource_type=NodeType.Model,
                unique_id='model.root.sibling',
                fqn=['root', 'sibling'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.events']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
            'model.root.multi': ParsedModelNode(
                name='multi',
                database='dbt',
                schema='analytics',
                alias='multi',
                resource_type=NodeType.Model,
                unique_id='model.root.multi',
                fqn=['root', 'multi'],
                package_name='root',
                refs=[['events']],
                sources=[],
                depends_on=DependsOn(nodes=['model.root.nested', 'model.root.sibling']),
                config=self.model_config,
                tags=[],
                path='multi.sql',
                original_file_path='multi.sql',
                root_path='',
                meta={},
                raw_sql='does not matter'
            ),
        }

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        self.assertEqual(
            manifest.writable_manifest().to_dict(),
            {
                'nodes': {},
                'macros': {},
                'sources': {},
                'parent_map': {},
                'child_map': {},
                'generated_at': '2018-02-14T09:15:13Z',
                'docs': {},
                'metadata': {},
                'disabled': [],
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        serialized = manifest.writable_manifest().to_dict()
        self.assertEqual(serialized['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['disabled'], [])
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

    def test__build_flat_graph(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, sources={}, macros={}, docs={},
                            generated_at=datetime.utcnow(), disabled=[],
                            files={})
        manifest.build_flat_graph()
        flat_graph = manifest.flat_graph
        flat_nodes = flat_graph['nodes']
        self.assertEqual(set(flat_graph), set(['nodes', 'sources']))
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        compiled_count = 0
        for node in flat_nodes.values():
            if node.get('compiled'):
                self.assertEqual(frozenset(node), REQUIRED_COMPILED_NODE_KEYS)
                compiled_count += 1
            else:
                self.assertEqual(frozenset(node), REQUIRED_PARSED_NODE_KEYS)
        self.assertEqual(compiled_count, 2)


# Tests of the manifest search code (find_X_by_Y)

class TestManifestSearch(unittest.TestCase):
    _macros = []
    _models = []
    _docs = []
    @property
    def macros(self):
        return self._macros

    @property
    def nodes(self):
        return self._nodes

    @property
    def docs(self):
        return self._docs

    def setUp(self):
        self.manifest = Manifest(
            nodes={
                n.unique_id: n for n in self.nodes
            },
            macros={
                m.unique_id: m for m in self.macros
            },
            docs={
                d.unique_id: d for d in self.docs
            },
            generated_at=datetime.utcnow(),
            disabled=[],
            files={}
        )


def make_manifest(nodes=[], sources=[], macros=[], docs=[]):
    return Manifest(
        nodes={
            n.unique_id: n for n in nodes
        },
        macros={
            m.unique_id: m for m in macros
        },
        sources={
            s.unique_id: s for s in sources
        },
        docs={
            d.unique_id: d for d in docs
        },
        generated_at=datetime.utcnow(),
        disabled=[],
        files={}
    )


FindMacroSpec = namedtuple('FindMacroSpec', 'macros,expected')

macro_parameter_sets = [
    # empty
    FindMacroSpec(
        macros=[],
        expected={None: None, 'root': None, 'dep': None, 'dbt': None},
    ),

    # just root
    FindMacroSpec(
        macros=[MockMacro('root')],
        expected={None: 'root', 'root': 'root', 'dep': None, 'dbt': None},
    ),

    # just dep
    FindMacroSpec(
        macros=[MockMacro('dep')],
        expected={None: 'dep', 'root': None, 'dep': 'dep', 'dbt': None},
    ),

    # just dbt
    FindMacroSpec(
        macros=[MockMacro('dbt')],
        expected={None: 'dbt', 'root': None, 'dep': None, 'dbt': 'dbt'},
    ),

    # root overrides dep
    FindMacroSpec(
        macros=[MockMacro('root'), MockMacro('dep')],
        expected={None: 'root', 'root': 'root', 'dep': 'dep', 'dbt': None},
    ),

    # root overrides core
    FindMacroSpec(
        macros=[MockMacro('root'), MockMacro('dbt')],
        expected={None: 'root', 'root': 'root', 'dep': None, 'dbt': 'dbt'},
    ),

    # dep overrides core
    FindMacroSpec(
        macros=[MockMacro('dep'), MockMacro('dbt')],
        expected={None: 'dep', 'root': None, 'dep': 'dep', 'dbt': 'dbt'},
    ),

    # root overrides dep overrides core
    FindMacroSpec(
        macros=[MockMacro('root'), MockMacro('dep'), MockMacro('dbt')],
        expected={None: 'root', 'root': 'root', 'dep': 'dep', 'dbt': 'dbt'},
    ),
]


def id_macro(arg):
    if isinstance(arg, list):
        macro_names = '__'.join(f'{m.package_name}' for m in arg)
        return f'm_[{macro_names}]'
    if isinstance(arg, dict):
        arg_names = '__'.join(f'{k}_{v}' for k, v in arg.items())
        return f'exp_{{{arg_names}}}'


@pytest.mark.parametrize('macros,expectations', macro_parameter_sets, ids=id_macro)
def test_find_macro_by_name(macros, expectations):
    manifest = make_manifest(macros=macros)
    for package, expected in expectations.items():
        result = manifest.find_macro_by_name(name='my_macro', root_project_name='root', package=package)
        if expected is None:
            assert result is expected
        else:
            assert result.package_name == expected


# these don't use a search package, so we don't need to do as much
generate_name_parameter_sets = [
    # empty
    FindMacroSpec(
        macros=[],
        expected=None,
    ),

    # just root
    FindMacroSpec(
        macros=[MockGenerateMacro('root')],
        expected='root',
    ),

    # just dep
    FindMacroSpec(
        macros=[MockGenerateMacro('dep')],
        expected=None,
    ),

    # just dbt
    FindMacroSpec(
        macros=[MockGenerateMacro('dbt')],
        expected='dbt',
    ),

    # root overrides dep
    FindMacroSpec(
        macros=[MockGenerateMacro('root'), MockGenerateMacro('dep')],
        expected='root',
    ),

    # root overrides core
    FindMacroSpec(
        macros=[MockGenerateMacro('root'), MockGenerateMacro('dbt')],
        expected='root',
    ),

    # dep overrides core
    FindMacroSpec(
        macros=[MockGenerateMacro('dep'), MockGenerateMacro('dbt')],
        expected='dbt',
    ),

    # root overrides dep overrides core
    FindMacroSpec(
        macros=[MockGenerateMacro('root'), MockGenerateMacro('dep'), MockGenerateMacro('dbt')],
        expected='root',
    ),
]


@pytest.mark.parametrize('macros,expected', generate_name_parameter_sets, ids=id_macro)
def test_find_generate_macro_by_name(macros, expected):
    manifest = make_manifest(macros=macros)
    result = manifest.find_generate_macro_by_name(
        component='some_component', root_project_name='root'
    )
    if expected is None:
        assert result is expected
    else:
        assert result.package_name == expected


FindMaterializationSpec = namedtuple('FindMaterializationSpec', 'macros,adapter_type,expected')


def _materialization_parameter_sets():
    sets = [
        FindMaterializationSpec(macros=[], adapter_type='foo', expected=None),
    ]

    # default only, each project
    sets.extend(
        FindMaterializationSpec(
            macros=[MockMaterialization(project, adapter_type=None)],
            adapter_type='foo',
            expected=(project, 'default'),
        ) for project in ['root', 'dep', 'dbt']
    )

    # other type only, each project
    sets.extend(
        FindMaterializationSpec(
            macros=[MockMaterialization(project, adapter_type='bar')],
            adapter_type='foo',
            expected=None,
        ) for project in ['root', 'dep', 'dbt']
    )

    # matching type only, each project
    sets.extend(
        FindMaterializationSpec(
            macros=[MockMaterialization(project, adapter_type='foo')],
            adapter_type='foo',
            expected=(project, 'foo'),
        ) for project in ['root', 'dep', 'dbt']
    )

    sets.extend([
        # matching type and default everywhere
        FindMaterializationSpec(
            macros=[MockMaterialization(project, adapter_type=atype) for (project, atype) in product(['root', 'dep', 'dbt'], ['foo', None])],
            adapter_type='foo',
            expected=('root', 'foo')
        ),
        # default in core, override is in dep, and root has unrelated override
        # should find the dep override.
        FindMaterializationSpec(
            macros=[MockMaterialization('root', adapter_type='bar'), MockMaterialization('dep', adapter_type='foo'), MockMaterialization('dbt', adapter_type=None)],
            adapter_type='foo',
            expected=('dep', 'foo'),
        ),
        # default in core, unrelated override is in dep, and root has an override
        # should find the root override.
        FindMaterializationSpec(
            macros=[MockMaterialization('root', adapter_type='foo'), MockMaterialization('dep', adapter_type='bar'), MockMaterialization('dbt', adapter_type=None)],
            adapter_type='foo',
            expected=('root', 'foo'),
        ),
        # default in core, override is in dep, and root has an override too.
        # should find the root override.
        FindMaterializationSpec(
            macros=[MockMaterialization('root', adapter_type='foo'), MockMaterialization('dep', adapter_type='foo'), MockMaterialization('dbt', adapter_type=None)],
            adapter_type='foo',
            expected=('root', 'foo'),
        ),
        # core has default + adapter, dep has adapter, root has default
        # should find the dependency implementation, because it's the most specific
        FindMaterializationSpec(
            macros=[
                MockMaterialization('root', adapter_type=None),
                MockMaterialization('dep', adapter_type='foo'),
                MockMaterialization('dbt', adapter_type=None),
                MockMaterialization('dbt', adapter_type='foo'),
            ],
            adapter_type='foo',
            expected=('dep', 'foo'),
        ),
    ])

    return sets


def id_mat(arg):
    if isinstance(arg, list):
        macro_names = '__'.join(f'{m.package_name}_{m.adapter_type}' for m in arg)
        return f'm_[{macro_names}]'
    elif isinstance(arg, tuple):
        return '_'.join(arg)


@pytest.mark.parametrize(
    'macros,adapter_type,expected',
    _materialization_parameter_sets(),
    ids=id_mat,
)
def test_find_materialization_by_name(macros, adapter_type, expected):
    manifest = make_manifest(macros=macros)
    result = manifest.find_materialization_macro_by_name(
        project_name='root',
        materialization_name='my_materialization',
        adapter_type=adapter_type,
    )
    if expected is None:
        assert result is expected
    else:
        expected_package, expected_adapter_type = expected
        assert result.adapter_type == expected_adapter_type
        assert result.package_name == expected_package


FindNodeSpec = namedtuple('FindNodeSpec', 'nodes,sources,package,expected')


def _refable_parameter_sets():
    sets = [
        # empties
        FindNodeSpec(nodes=[], sources=[], package=None, expected=None),
        FindNodeSpec(nodes=[], sources=[], package='root', expected=None),
    ]
    sets.extend(
        # only one model, no package specified -> find it in any package
        FindNodeSpec(
            nodes=[MockNode(project, 'my_model')],
            sources=[],
            package=None,
            expected=(project, 'my_model'),
        ) for project in ['root', 'dep']
    )
    # only one model, no package specified -> find it in any package
    sets.extend([
        FindNodeSpec(
            nodes=[MockNode('root', 'my_model')],
            sources=[],
            package='root',
            expected=('root', 'my_model'),
        ),
        FindNodeSpec(
            nodes=[MockNode('dep', 'my_model')],
            sources=[],
            package='root',
            expected=None,
        ),

        # a source with that name exists, but not a refable
        FindNodeSpec(
            nodes=[],
            sources=[MockSource('root', 'my_source', 'my_model')],
            package=None,
            expected=None
        ),

        # a source with that name exists, and a refable
        FindNodeSpec(
            nodes=[MockNode('root', 'my_model')],
            sources=[MockSource('root', 'my_source', 'my_model')],
            package=None,
            expected=('root', 'my_model'),
        ),
        FindNodeSpec(
            nodes=[MockNode('root', 'my_model')],
            sources=[MockSource('root', 'my_source', 'my_model')],
            package='root',
            expected=('root', 'my_model'),
        ),
        FindNodeSpec(
            nodes=[MockNode('root', 'my_model')],
            sources=[MockSource('root', 'my_source', 'my_model')],
            package='dep',
            expected=None,
        ),

    ])
    return sets


def id_nodes(arg):
    if isinstance(arg, list):
        node_names = '__'.join(f'{n.package_name}_{n.search_name}' for n in arg)
        return f'm_[{node_names}]'
    elif isinstance(arg, tuple):
        return '_'.join(arg)


@pytest.mark.parametrize(
    'nodes,sources,package,expected',
    _refable_parameter_sets(),
    ids=id_nodes,
)
def test_resolve_ref(nodes, sources, package, expected):
    manifest = make_manifest(nodes=nodes, sources=sources)
    result = manifest.resolve_ref(
        target_model_name='my_model',
        target_model_package=package,
        current_project='root',
        node_package='root',
    )
    if expected is None:
        assert result is expected
    else:
        assert result is not None
        assert len(expected) == 2
        expected_package, expected_name = expected
        assert result.name == expected_name
        assert result.package_name == expected_package


def _source_parameter_sets():
    sets = [
        # empties
        FindNodeSpec(nodes=[], sources=[], package='dep', expected=None),
        FindNodeSpec(nodes=[], sources=[], package='root', expected=None),
    ]
    sets.extend(
        # models with the name, but not sources
        FindNodeSpec(
            nodes=[MockNode('root', name)],
            sources=[],
            package=project,
            expected=None,
        )
        for project in ('root', 'dep') for name in ('my_source', 'my_table')
    )
    # exists in root alongside nodes with name parts
    sets.extend(
        FindNodeSpec(
            nodes=[MockNode('root', 'my_source'), MockNode('root', 'my_table')],
            sources=[MockSource('root', 'my_source', 'my_table')],
            package=project,
            expected=('root', 'my_source', 'my_table'),
        )
        for project in ('root', 'dep')
    )
    sets.extend(
        # wrong source name
        FindNodeSpec(
            nodes=[],
            sources=[MockSource('root', 'my_other_source', 'my_table')],
            package=project,
            expected=None,
        )
        for project in ('root', 'dep')
    )
    sets.extend(
        # wrong table name
        FindNodeSpec(
            nodes=[],
            sources=[MockSource('root', 'my_source', 'my_other_table')],
            package=project,
            expected=None,
        )
        for project in ('root', 'dep')
    )
    sets.append(
        # should be found by the package=None search
        FindNodeSpec(
            nodes=[],
            sources=[MockSource('other', 'my_source', 'my_table')],
            package='root',
            expected=('other', 'my_source', 'my_table'),
        )
    )
    sets.extend(
        # exists in root check various projects (other project -> not found)
        FindNodeSpec(
            nodes=[],
            sources=[MockSource('root', 'my_source', 'my_table')],
            package=project,
            expected=('root', 'my_source', 'my_table'),
        )
        for project in ('root', 'dep')
    )

    return sets


@pytest.mark.parametrize(
    'nodes,sources,package,expected',
    _source_parameter_sets(),
    ids=id_nodes,
)
def test_resolve_source(nodes, sources, package, expected):
    manifest = make_manifest(nodes=nodes, sources=sources)
    result = manifest.resolve_source(
        target_source_name='my_source',
        target_table_name='my_table',
        current_project=package,
        node_package='dep',
    )
    if expected is None:
        assert result is expected
    else:
        assert result is not None
        assert len(expected) == 3
        expected_package, expected_source_name, expected_name = expected
        assert result.source_name == expected_source_name
        assert result.name == expected_name
        assert result.package_name == expected_package


FindDocSpec = namedtuple('FindDocSpec', 'docs,package,expected')


def _docs_parameter_sets():
    sets = []
    sets.extend(
        # empty
        FindDocSpec(docs=[], package=project, expected=None)
        for project in ('root', None)
    )
    sets.extend(
        # basic: exists in root
        FindDocSpec(docs=[MockDocumentation('root', 'my_doc')], package=project, expected=('root', 'my_doc'))
        for project in ('root', None)
    )
    sets.extend([
        # exists in other
        FindDocSpec(docs=[MockDocumentation('dep', 'my_doc')], package='root', expected=None),
        FindDocSpec(docs=[MockDocumentation('dep', 'my_doc')], package=None, expected=('dep', 'my_doc')),
    ])
    return sets


@pytest.mark.parametrize(
    'docs,package,expected',
    _docs_parameter_sets(),
    ids=id_nodes,
)
def test_resolve_doc(docs, package, expected):
    manifest = make_manifest(docs=docs)
    result = manifest.resolve_doc(name='my_doc', package=package, current_project='root', node_package='root')
    if expected is None:
        assert result is expected
    else:
        assert result is not None
        assert len(expected) == 2
        expected_package, expected_name = expected
        assert result.name == expected_name
        assert result.package_name == expected_package
