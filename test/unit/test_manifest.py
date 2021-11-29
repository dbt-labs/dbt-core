import os
import unittest
from unittest import mock

import copy
from collections import namedtuple
from itertools import product
from datetime import datetime

import pytest

import dbt.flags
import dbt.version
from dbt import tracking
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest, ManifestMetadata
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
    DependsOn,
    NodeConfig,
    ParsedSeedNode,
    ParsedSourceDefinition,
    ParsedExposure,
    ParsedMetric
)

from dbt.contracts.graph.unparsed import (
    ExposureType,
    ExposureOwner,
    MaturityType,
    MetricFilter
)

from dbt.contracts.graph.compiled import CompiledModelNode
from dbt.events.functions import get_invocation_id
from dbt.node_types import NodeType
import freezegun

from .utils import MockMacro, MockDocumentation, MockSource, MockNode, MockMaterialization, MockGenerateMacro


REQUIRED_PARSED_NODE_KEYS = frozenset({
    'alias', 'tags', 'config', 'unique_id', 'refs', 'sources', 'meta',
    'depends_on', 'database', 'schema', 'name', 'resource_type',
    'package_name', 'root_path', 'path', 'original_file_path', 'raw_sql',
    'description', 'columns', 'fqn', 'build_path', 'compiled_path', 'patch_path', 'docs',
    'deferred', 'checksum', 'unrendered_config', 'created_at',
})

REQUIRED_COMPILED_NODE_KEYS = frozenset(REQUIRED_PARSED_NODE_KEYS | {
    'compiled', 'extra_ctes_injected', 'extra_ctes', 'compiled_sql',
    'relation_name'
})


ENV_KEY_NAME = 'KEY' if os.name == 'nt' else 'key'


class ManifestTest(unittest.TestCase):
    def setUp(self):
        # TODO: why is this needed for tests in this module to pass?
        tracking.active_user = None

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

        self.exposures = {
            'exposure.root.my_exposure': ParsedExposure(
                name='my_exposure',
                type=ExposureType.Dashboard,
                owner=ExposureOwner(email='some@email.com'),
                resource_type=NodeType.Exposure,
                description='Test description',
                maturity=MaturityType.High,
                url='hhtp://mydashboard.com',
                depends_on=DependsOn(nodes=['model.root.multi']),
                refs=[['multi']],
                sources=[],
                fqn=['root', 'my_exposure'],
                unique_id='exposure.root.my_exposure',
                package_name='root',
                root_path='',
                path='my_exposure.sql',
                original_file_path='my_exposure.sql'
            )
        }

        self.metrics = {
            'metric.root.my_metric': ParsedMetric(
                name='new_customers',
                label='New Customers',
                model='ref("multi")',
                description="New customers",
                type='count',
                sql="user_id",
                timestamp="signup_date",
                time_grains=['day', 'week', 'month'],
                dimensions=['plan', 'country'],
                filters=[MetricFilter(
                   field="is_paying",
                   value='True',
                   operator="=",
                )],
                meta={'is_okr': True},
                tags=['okrs'],
                resource_type=NodeType.Metric,
                depends_on=DependsOn(nodes=['model.root.multi']),
                refs=[['multi']],
                sources=[],
                fqn=['root', 'my_metric'],
                unique_id='metric.root.my_metric',
                package_name='root',
                root_path='',
                path='my_metric.yml',
                original_file_path='my_metric.yml'
            )
        }

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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
        for exposure in self.exposures.values():
            exposure.validate(exposure.to_dict(omit_none=True))
        for metric in self.metrics.values():
            metric.validate(metric.to_dict(omit_none=True))
        for node in self.nested_nodes.values():
            node.validate(node.to_dict(omit_none=True))
        for source in self.sources.values():
            source.validate(source.to_dict(omit_none=True))

        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

    def tearDown(self):
        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        manifest = Manifest(
            nodes={}, sources={}, macros={}, docs={}, disabled={}, files={},
            exposures={}, metrics={}, selectors={},
            metadata=ManifestMetadata(generated_at=datetime.utcnow()),
        )

        invocation_id = dbt.events.functions.invocation_id
        self.assertEqual(
            manifest.writable_manifest().to_dict(omit_none=True),
            {
                'nodes': {},
                'sources': {},
                'macros': {},
                'exposures': {},
                'metrics': {},
                'selectors': {},
                'parent_map': {},
                'child_map': {},
                'metadata': {
                    'generated_at': '2018-02-14T09:15:13Z',
                    'dbt_schema_version': 'https://schemas.getdbt.com/dbt/manifest/v4.json',
                    'dbt_version': dbt.version.__version__,
                    'env': {ENV_KEY_NAME: 'value'},
                    'invocation_id': invocation_id,
                },
                'docs': {},
                'disabled': {},
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(
            nodes=nodes, sources={}, macros={}, docs={}, disabled={}, files={},
            exposures={}, metrics={}, selectors={},
            metadata=ManifestMetadata(generated_at=datetime.utcnow()),
        )
        serialized = manifest.writable_manifest().to_dict(omit_none=True)
        self.assertEqual(serialized['metadata']['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['docs'], {})
        self.assertEqual(serialized['disabled'], {})
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
        exposures = copy.copy(self.exposures)
        metrics = copy.copy(self.metrics)
        nodes = copy.copy(self.nested_nodes)
        sources = copy.copy(self.sources)
        manifest = Manifest(nodes=nodes, sources=sources, macros={}, docs={},
                            disabled={}, files={}, exposures=exposures,
                            metrics=metrics, selectors={})
        manifest.build_flat_graph()
        flat_graph = manifest.flat_graph
        flat_exposures = flat_graph['exposures']
        flat_metrics = flat_graph['metrics']
        flat_nodes = flat_graph['nodes']
        flat_sources = flat_graph['sources']
        self.assertEqual(set(flat_graph), set(['exposures', 'nodes', 'sources', 'metrics']))
        self.assertEqual(set(flat_exposures), set(self.exposures))
        self.assertEqual(set(flat_metrics), set(self.metrics))
        self.assertEqual(set(flat_nodes), set(self.nested_nodes))
        self.assertEqual(set(flat_sources), set(self.sources))
        for node in flat_nodes.values():
            self.assertEqual(frozenset(node), REQUIRED_PARSED_NODE_KEYS)

    @mock.patch.object(tracking, 'active_user')
    def test_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        dbt.events.functions.invocation_id = '01234567-0123-0123-0123-0123456789ab'
        dbt.flags.SEND_ANONYMOUS_USAGE_STATS = False
        now = datetime.utcnow()
        self.assertEqual(
            ManifestMetadata(
                project_id='098f6bcd4621d373cade4e832627b4f6',
                adapter_type='postgres',
                generated_at=now,
            ),
            ManifestMetadata(
                project_id='098f6bcd4621d373cade4e832627b4f6',
                user_id='cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                send_anonymous_usage_stats=False,
                adapter_type='postgres',
                generated_at=now,
                invocation_id='01234567-0123-0123-0123-0123456789ab',
            )
        )

    @mock.patch.object(tracking, 'active_user')
    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test_no_nodes_with_metadata(self, mock_user):
        mock_user.id = 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf'
        dbt.events.functions.invocation_id = '01234567-0123-0123-0123-0123456789ab'
        dbt.flags.SEND_ANONYMOUS_USAGE_STATS = False
        metadata = ManifestMetadata(
            project_id='098f6bcd4621d373cade4e832627b4f6',
            adapter_type='postgres',
            generated_at=datetime.utcnow(),
        )
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            disabled={}, selectors={},
                            metadata=metadata, files={}, exposures={})

        self.assertEqual(
            manifest.writable_manifest().to_dict(omit_none=True),
            {
                'nodes': {},
                'sources': {},
                'macros': {},
                'exposures': {},
                'metrics': {},
                'selectors': {},
                'parent_map': {},
                'child_map': {},
                'docs': {},
                'metadata': {
                    'generated_at': '2018-02-14T09:15:13Z',
                    'dbt_schema_version': 'https://schemas.getdbt.com/dbt/manifest/v4.json',
                    'dbt_version': dbt.version.__version__,
                    'project_id': '098f6bcd4621d373cade4e832627b4f6',
                    'user_id': 'cfc9500f-dc7f-4c83-9ea7-2c581c1b38cf',
                    'send_anonymous_usage_stats': False,
                    'adapter_type': 'postgres',
                    'invocation_id': '01234567-0123-0123-0123-0123456789ab',
                    'env': {ENV_KEY_NAME: 'value'},
                },
                'disabled': {},
            }
        )

    def test_get_resource_fqns_empty(self):
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={},
                            disabled={}, files={}, exposures={}, selectors={})
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
            checksum=FileHash.empty(),
        )
        manifest = Manifest(nodes=nodes, sources=self.sources, macros={}, docs={},
                            disabled={}, files={}, exposures=self.exposures,
                            metrics=self.metrics, selectors={})
        expect = {
            'metrics': frozenset([
                ('root', 'my_metric')
            ]),
            'exposures': frozenset([
                ('root', 'my_exposure')
            ]),
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
                relation_name='"dbt"."analytics"."events"',
                extra_ctes=[],
                checksum=FileHash.empty(),
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
                relation_name='"dbt"."analytics"."events"',
                extra_ctes=[],
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
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
                raw_sql='does not matter',
                checksum=FileHash.empty(),
            ),
        }
        os.environ['DBT_ENV_CUSTOM_ENV_key'] = 'value'

    def tearDown(self):
        del os.environ['DBT_ENV_CUSTOM_ENV_key']

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__no_nodes(self):
        metadata = ManifestMetadata(generated_at=datetime.utcnow(), invocation_id='01234567-0123-0123-0123-0123456789ab')
        manifest = Manifest(nodes={}, sources={}, macros={}, docs={}, selectors={},
                            disabled={}, metadata=metadata, files={}, exposures={})
        self.assertEqual(
            manifest.writable_manifest().to_dict(omit_none=True),
            {
                'nodes': {},
                'macros': {},
                'sources': {},
                'exposures': {},
                'metrics': {},
                'selectors': {},
                'parent_map': {},
                'child_map': {},
                'metadata': {
                    'generated_at': '2018-02-14T09:15:13Z',
                    'dbt_schema_version': 'https://schemas.getdbt.com/dbt/manifest/v4.json',
                    'dbt_version': dbt.version.__version__,
                    'invocation_id': '01234567-0123-0123-0123-0123456789ab',
                    'env': {ENV_KEY_NAME: 'value'},
                },
                'docs': {},
                'disabled': {},
            }
        )

    @freezegun.freeze_time('2018-02-14T09:15:13Z')
    def test__nested_nodes(self):
        nodes = copy.copy(self.nested_nodes)
        manifest = Manifest(nodes=nodes, sources={}, macros={}, docs={},
                            disabled={}, selectors={},
                            metadata=ManifestMetadata(generated_at=datetime.utcnow()),
                            files={}, exposures={})
        serialized = manifest.writable_manifest().to_dict(omit_none=True)
        self.assertEqual(serialized['metadata']['generated_at'], '2018-02-14T09:15:13Z')
        self.assertEqual(serialized['disabled'], {})
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
                            disabled={}, selectors={},
                            files={}, exposures={})
        manifest.build_flat_graph()
        flat_graph = manifest.flat_graph
        flat_nodes = flat_graph['nodes']
        self.assertEqual(set(flat_graph), set(['exposures', 'metrics', 'nodes', 'sources']))
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
    _nodes = []
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
            disabled={},
            files={},
            exposures={},
            metrics={},
            selectors={},
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
        disabled={},
        files={},
        exposures={},
        metrics={},
        selectors={},
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
