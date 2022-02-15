import itertools
import unittest
import os
from typing import Set, Dict, Any
from unittest import mock

import pytest

from dbt.adapters import postgres
from dbt.adapters import factory
from dbt.adapters.base import AdapterConfig
from dbt.clients.jinja import MacroStack
from dbt.contracts.graph.parsed import (
    ParsedModelNode, NodeConfig, DependsOn, ParsedMacro
)
from dbt.config.project import VarProvider
from dbt.context import base, target, configured, providers, docs, manifest, macros
from dbt.contracts.files import FileHash
from dbt.node_types import NodeType
import dbt.exceptions
from .utils import profile_from_dict, config_from_parts_or_dicts, inject_adapter, clear_plugin
from .mock_adapter import adapter_factory


class TestVar(unittest.TestCase):
    def setUp(self):
        self.model = ParsedModelNode(
            alias='model_one',
            name='model_one',
            database='dbt',
            schema='analytics',
            resource_type=NodeType.Model,
            unique_id='model.root.model_one',
            fqn=['root', 'model_one'],
            package_name='root',
            original_file_path='model_one.sql',
            root_path='/usr/src/app',
            refs=[],
            sources=[],
            depends_on=DependsOn(),
            config=NodeConfig.from_dict({
                'enabled': True,
                'materialized': 'view',
                'persist_docs': {},
                'post-hook': [],
                'pre-hook': [],
                'vars': {},
                'quoting': {},
                'column_types': {},
                'tags': [],
            }),
            tags=[],
            path='model_one.sql',
            raw_sql='',
            description='',
            columns={},
            checksum=FileHash.from_contents(''),
        )
        self.context = mock.MagicMock()
        self.provider = VarProvider({})
        self.config = mock.MagicMock(
            config_version=2, vars=self.provider, cli_vars={}, project_name='root'
        )

    def test_var_default_something(self):
        self.config.cli_vars = {'foo': 'baz'}
        var = providers.RuntimeVar(self.context, self.config, self.model)
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_var_default_none(self):
        self.config.cli_vars = {'foo': None}
        var = providers.RuntimeVar(self.context, self.config, self.model)
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_var_not_defined(self):
        var = providers.RuntimeVar(self.context, self.config, self.model)

        self.assertEqual(var('foo', 'bar'), 'bar')
        with self.assertRaises(dbt.exceptions.CompilationException):
            var('foo')

    def test_parser_var_default_something(self):
        self.config.cli_vars = {'foo': 'baz'}
        var = providers.ParseVar(self.context, self.config, self.model)
        self.assertEqual(var('foo'), 'baz')
        self.assertEqual(var('foo', 'bar'), 'baz')

    def test_parser_var_default_none(self):
        self.config.cli_vars = {'foo': None}
        var = providers.ParseVar(self.context, self.config, self.model)
        self.assertEqual(var('foo'), None)
        self.assertEqual(var('foo', 'bar'), None)

    def test_parser_var_not_defined(self):
        # at parse-time, we should not raise if we encounter a missing var
        # that way disabled models don't get parse errors
        var = providers.ParseVar(self.context, self.config, self.model)

        self.assertEqual(var('foo', 'bar'), 'bar')
        self.assertEqual(var('foo'), None)


class TestParseWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_config = mock.MagicMock()
        adapter_class = adapter_factory()
        self.mock_adapter = adapter_class(self.mock_config)
        self.namespace = mock.MagicMock()
        self.wrapper = providers.ParseDatabaseWrapper(
            self.mock_adapter, self.namespace)
        self.responder = self.mock_adapter.responder

    def test_unwrapped_method(self):
        self.assertEqual(self.wrapper.quote('test_value'), '"test_value"')
        self.responder.quote.assert_called_once_with('test_value')

    def test_wrapped_method(self):
        found = self.wrapper.get_relation('database', 'schema', 'identifier')
        self.assertEqual(found, None)
        self.responder.get_relation.assert_not_called()


class TestRuntimeWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_config = mock.MagicMock()
        self.mock_config.quoting = {
            'database': True, 'schema': True, 'identifier': True}
        adapter_class = adapter_factory()
        self.mock_adapter = adapter_class(self.mock_config)
        self.namespace = mock.MagicMock()
        self.wrapper = providers.RuntimeDatabaseWrapper(
            self.mock_adapter, self.namespace)
        self.responder = self.mock_adapter.responder

    def test_unwrapped_method(self):
        # the 'quote' method isn't wrapped, we should get our expected inputs
        self.assertEqual(self.wrapper.quote('test_value'), '"test_value"')
        self.responder.quote.assert_called_once_with('test_value')

    def test_wrapped_method(self):
        rel = mock.MagicMock()
        rel.matches.return_value = True
        self.responder.list_relations_without_caching.return_value = [rel]

        found = self.wrapper.get_relation('database', 'schema', 'identifier')

        self.assertEqual(found, rel)

        self.responder.list_relations_without_caching.assert_called_once_with(
            mock.ANY)
        # extract the argument
        assert len(self.responder.list_relations_without_caching.mock_calls) == 1
        assert len(
            self.responder.list_relations_without_caching.call_args[0]) == 1
        arg = self.responder.list_relations_without_caching.call_args[0][0]
        assert arg.database == 'database'
        assert arg.schema == 'schema'


def assert_has_keys(
    required_keys: Set[str], maybe_keys: Set[str], ctx: Dict[str, Any]
):
    keys = set(ctx)
    for key in required_keys:
        assert key in keys, f'{key} in required keys but not in context'
        keys.remove(key)
    extras = keys.difference(maybe_keys)
    assert not extras, f'got extra keys in context: {extras}'


REQUIRED_BASE_KEYS = frozenset({
    'context',
    'builtins',
    'dbt_version',
    'var',
    'env_var',
    'return',
    'fromjson',
    'tojson',
    'fromyaml',
    'toyaml',
    'log',
    'run_started_at',
    'invocation_id',
    'modules',
    'flags',
    'print',
})

REQUIRED_TARGET_KEYS = REQUIRED_BASE_KEYS | {'target'}
REQUIRED_DOCS_KEYS = REQUIRED_TARGET_KEYS | {'project_name'} | {'doc'}
MACROS = frozenset({'macro_a', 'macro_b', 'root', 'dbt'})
REQUIRED_QUERY_HEADER_KEYS = REQUIRED_TARGET_KEYS | {'project_name'} | MACROS
REQUIRED_MACRO_KEYS = REQUIRED_QUERY_HEADER_KEYS | {
    '_sql_results',
    'load_result',
    'store_result',
    'store_raw_result',
    'validation',
    'write',
    'render',
    'try_or_compiler_error',
    'load_agate_table',
    'ref',
    'source',
    'config',
    'execute',
    'exceptions',
    'database',
    'schema',
    'adapter',
    'api',
    'column',
    'env',
    'graph',
    'model',
    'pre_hooks',
    'post_hooks',
    'sql',
    'sql_now',
    'adapter_macro',
}
REQUIRED_MODEL_KEYS = REQUIRED_MACRO_KEYS | {'this'}
MAYBE_KEYS = frozenset({'debug'})


POSTGRES_PROFILE_DATA = {
    'target': 'test',
    'quoting': {},
    'outputs': {
        'test': {
            'type': 'postgres',
            'host': 'localhost',
            'schema': 'analytics',
            'user': 'test',
            'pass': 'test',
            'dbname': 'test',
            'port': 1,
        }
    },
}

PROJECT_DATA = {
    'name': 'root',
    'version': '0.1',
    'profile': 'test',
    'project-root': os.getcwd(),
    'config-version': 2,
}


def model():
    return ParsedModelNode(
        alias='model_one',
        name='model_one',
        database='dbt',
        schema='analytics',
        resource_type=NodeType.Model,
        unique_id='model.root.model_one',
        fqn=['root', 'model_one'],
        package_name='root',
        original_file_path='model_one.sql',
        root_path='/usr/src/app',
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }),
        tags=[],
        path='model_one.sql',
        raw_sql='',
        description='',
        columns={}
    )


def test_base_context():
    ctx = base.generate_base_context({})
    assert_has_keys(REQUIRED_BASE_KEYS, MAYBE_KEYS, ctx)


def mock_macro(name, package_name):
    macro = mock.MagicMock(
        __class__=ParsedMacro,
        package_name=package_name,
        resource_type='macro',
        unique_id=f'macro.{package_name}.{name}',
    )
    # Mock(name=...) does not set the `name` attribute, this does.
    macro.name = name
    return macro


def mock_manifest(config):
    manifest_macros = {}
    for name in ['macro_a', 'macro_b']:
        macro = mock_macro(name, config.project_name)
        manifest_macros[macro.unique_id] = macro
    return mock.MagicMock(macros=manifest_macros)


def mock_model():
    return mock.MagicMock(
        __class__=ParsedModelNode,
        alias='model_one',
        name='model_one',
        database='dbt',
        schema='analytics',
        resource_type=NodeType.Model,
        unique_id='model.root.model_one',
        fqn=['root', 'model_one'],
        package_name='root',
        original_file_path='model_one.sql',
        root_path='/usr/src/app',
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict({
            'enabled': True,
            'materialized': 'view',
            'persist_docs': {},
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
            'quoting': {},
            'column_types': {},
            'tags': [],
        }),
        tags=[],
        path='model_one.sql',
        raw_sql='',
        description='',
        columns={},
    )


@pytest.fixture
def get_adapter():
    with mock.patch.object(providers, 'get_adapter') as patch:
        yield patch


@pytest.fixture
def get_include_paths():
    with mock.patch.object(factory, 'get_include_paths') as patch:
        patch.return_value = []
        yield patch


@pytest.fixture
def config_postgres():
    return config_from_parts_or_dicts(PROJECT_DATA, POSTGRES_PROFILE_DATA)


@pytest.fixture
def manifest_fx(config_postgres):
    return mock_manifest(config_postgres)


@pytest.fixture
def postgres_adapter(config_postgres, get_adapter):
    adapter = postgres.PostgresAdapter(config_postgres)
    inject_adapter(adapter, postgres.Plugin)
    get_adapter.return_value = adapter
    yield adapter
    clear_plugin(postgres.Plugin)


def test_query_header_context(config_postgres, manifest_fx):
    ctx = manifest.generate_query_header_context(
        config=config_postgres,
        manifest=manifest_fx,
    )
    assert_has_keys(REQUIRED_QUERY_HEADER_KEYS, MAYBE_KEYS, ctx)


def test_macro_runtime_context(config_postgres, manifest_fx, get_adapter, get_include_paths):
    ctx = providers.generate_runtime_macro_context(
        macro=manifest_fx.macros['macro.root.macro_a'],
        config=config_postgres,
        manifest=manifest_fx,
        package_name='root',
    )
    assert_has_keys(REQUIRED_MACRO_KEYS, MAYBE_KEYS, ctx)


def test_model_parse_context(config_postgres, manifest_fx, get_adapter, get_include_paths):
    ctx = providers.generate_parser_model_context(
        model=mock_model(),
        config=config_postgres,
        manifest=manifest_fx,
        context_config=mock.MagicMock(),
    )
    assert_has_keys(REQUIRED_MODEL_KEYS, MAYBE_KEYS, ctx)


def test_model_runtime_context(config_postgres, manifest_fx, get_adapter, get_include_paths):
    ctx = providers.generate_runtime_model_context(
        model=mock_model(),
        config=config_postgres,
        manifest=manifest_fx,
    )
    assert_has_keys(REQUIRED_MODEL_KEYS, MAYBE_KEYS, ctx)


def test_docs_runtime_context(config_postgres):
    ctx = docs.generate_runtime_docs_context(config_postgres, mock_model(), [], 'root')
    assert_has_keys(REQUIRED_DOCS_KEYS, MAYBE_KEYS, ctx)


def test_macro_namespace_duplicates(config_postgres, manifest_fx):
    mn = macros.MacroNamespaceBuilder(
        'root', 'search', MacroStack(), ['dbt_postgres', 'dbt']
    )
    mn.add_macros(manifest_fx.macros.values(), {})

    # same pkg, same name: error
    with pytest.raises(dbt.exceptions.CompilationException):
        mn.add_macro(mock_macro('macro_a', 'root'), {})

    # different pkg, same name: no error
    mn.add_macros(mock_macro('macro_a', 'dbt'), {})


def test_macro_namespace(config_postgres, manifest_fx):
    mn = macros.MacroNamespaceBuilder(
        'root', 'search', MacroStack(), ['dbt_postgres', 'dbt'])

    dbt_macro = mock_macro('some_macro', 'dbt')
    # same namespace, same name, different pkg!
    pg_macro = mock_macro('some_macro', 'dbt_postgres')
    # same name, different package
    package_macro = mock_macro('some_macro', 'root')

    all_macros = itertools.chain(manifest_fx.macros.values(), [
                                 dbt_macro, pg_macro, package_macro])

    namespace = mn.build_namespace(all_macros, {})
    dct = dict(namespace)
    for result in [dct, namespace]:
        assert 'dbt' in result
        assert 'root' in result
        assert 'some_macro' in result
        assert 'dbt_postgres' not in result
        # tests __len__
        assert len(result) == 5
        # tests __iter__
        assert set(result) == {'dbt', 'root',
                               'some_macro', 'macro_a', 'macro_b'}
        assert len(result['dbt']) == 1
        # from the regular manifest + some_macro
        assert len(result['root']) == 3
        assert result['dbt']['some_macro'].macro is pg_macro
        assert result['root']['some_macro'].macro is package_macro
        assert result['some_macro'].macro is package_macro
