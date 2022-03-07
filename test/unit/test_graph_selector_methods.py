import copy
import pytest
from unittest import mock

from pathlib import Path

from dbt.contracts.files import FileHash
from dbt.contracts.graph.parsed import (
    DependsOn,
    MacroDependsOn,
    NodeConfig,
    ParsedMacro,
    ParsedModelNode,
    ParsedExposure,
    ParsedMetric,
    ParsedSeedNode,
    ParsedSingularTestNode,
    ParsedGenericTestNode,
    ParsedSourceDefinition,
    TestConfig,
    TestMetadata,
    ColumnInfo,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.unparsed import ExposureType, ExposureOwner, MetricFilter
from dbt.contracts.state import PreviousState
from dbt.node_types import NodeType
from dbt.graph.selector_methods import (
    MethodManager,
    QualifiedNameSelectorMethod,
    TagSelectorMethod,
    SourceSelectorMethod,
    PathSelectorMethod,
    PackageSelectorMethod,
    ConfigSelectorMethod,
    TestNameSelectorMethod,
    TestTypeSelectorMethod,
    StateSelectorMethod,
    ExposureSelectorMethod,
    MetricSelectorMethod,
)
import dbt.exceptions
import dbt.contracts.graph.parsed
from .utils import replace_config


def make_model(pkg, name, sql, refs=None, sources=None, tags=None, path=None, alias=None, config_kwargs=None, fqn_extras=None, depends_on_macros=None):
    if refs is None:
        refs = []
    if sources is None:
        sources = []
    if tags is None:
        tags = []
    if path is None:
        path = f'{name}.sql'
    if alias is None:
        alias = name
    if config_kwargs is None:
        config_kwargs = {}
    if depends_on_macros is None:
        depends_on_macros = []

    if fqn_extras is None:
        fqn_extras = []

    fqn = [pkg] + fqn_extras + [name]

    depends_on_nodes = []
    source_values = []
    ref_values = []
    for ref in refs:
        ref_values.append([ref.name])
        depends_on_nodes.append(ref.unique_id)
    for src in sources:
        source_values.append([src.source_name, src.name])
        depends_on_nodes.append(src.unique_id)

    return ParsedModelNode(
        raw_sql=sql,
        database='dbt',
        schema='dbt_schema',
        alias=alias,
        name=name,
        fqn=fqn,
        unique_id=f'model.{pkg}.{name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=path,
        original_file_path=f'models/{path}',
        config=NodeConfig(**config_kwargs),
        tags=tags,
        refs=ref_values,
        sources=source_values,
        depends_on=DependsOn(
            nodes=depends_on_nodes,
            macros=depends_on_macros,
        ),
        resource_type=NodeType.Model,
        checksum=FileHash.from_contents(''),
    )


def make_seed(pkg, name, path=None, loader=None, alias=None, tags=None, fqn_extras=None, checksum=None):
    if alias is None:
        alias = name
    if tags is None:
        tags = []
    if path is None:
        path = f'{name}.csv'

    if fqn_extras is None:
        fqn_extras = []

    if checksum is None:
        checksum = FileHash.from_contents('')

    fqn = [pkg] + fqn_extras + [name]
    return ParsedSeedNode(
        raw_sql='',
        database='dbt',
        schema='dbt_schema',
        alias=alias,
        name=name,
        fqn=fqn,
        unique_id=f'seed.{pkg}.{name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=path,
        original_file_path=f'data/{path}',
        tags=tags,
        resource_type=NodeType.Seed,
        checksum=FileHash.from_contents(''),
    )


def make_source(pkg, source_name, table_name, path=None, loader=None, identifier=None, fqn_extras=None):
    if path is None:
        path = 'models/schema.yml'
    if loader is None:
        loader = 'my_loader'
    if identifier is None:
        identifier = table_name

    if fqn_extras is None:
        fqn_extras = []

    fqn = [pkg] + fqn_extras + [source_name, table_name]

    return ParsedSourceDefinition(
        fqn=fqn,
        database='dbt',
        schema='dbt_schema',
        unique_id=f'source.{pkg}.{source_name}.{table_name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=path,
        original_file_path=path,
        name=table_name,
        source_name=source_name,
        loader='my_loader',
        identifier=identifier,
        resource_type=NodeType.Source,
        loaded_at_field='loaded_at',
        tags=[],
        source_description='',
    )


def make_macro(pkg, name, macro_sql, path=None, depends_on_macros=None):
    if path is None:
        path = 'macros/macros.sql'
    
    if depends_on_macros is None:
        depends_on_macros = []
    
    return ParsedMacro(
        name=name,
        macro_sql=macro_sql,
        unique_id=f'macro.{pkg}.{name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=path,
        original_file_path=path,
        resource_type=NodeType.Macro,
        tags=[],
        depends_on=MacroDependsOn(macros=depends_on_macros),
    )


def make_unique_test(pkg, test_model, column_name, path=None, refs=None, sources=None, tags=None):
    return make_schema_test(pkg, 'unique', test_model, {}, column_name=column_name)


def make_not_null_test(pkg, test_model, column_name, path=None, refs=None, sources=None, tags=None):
    return make_schema_test(pkg, 'not_null', test_model, {}, column_name=column_name)


def make_schema_test(pkg, test_name, test_model, test_kwargs, path=None, refs=None, sources=None, tags=None, column_name=None):
    kwargs = test_kwargs.copy()
    ref_values = []
    source_values = []
    # this doesn't really have to be correct
    if isinstance(test_model, ParsedSourceDefinition):
        kwargs['model'] = "{{ source('" + test_model.source_name + \
            "', '" + test_model.name + "') }}"
        source_values.append([test_model.source_name, test_model.name])
    else:
        kwargs['model'] = "{{ ref('" + test_model.name + "')}}"
        ref_values.append([test_model.name])
    if column_name is not None:
        kwargs['column_name'] = column_name

    # whatever
    args_name = test_model.search_name.replace(".", "_")
    if column_name is not None:
        args_name += '_' + column_name
    node_name = f'{test_name}_{args_name}'
    raw_sql = '{{ config(severity="ERROR") }}{{ test_' + \
        test_name + '(**dbt_schema_test_kwargs) }}'
    name_parts = test_name.split('.')

    if len(name_parts) == 2:
        namespace, test_name = name_parts
        macro_depends = f'macro.{namespace}.test_{test_name}'
    elif len(name_parts) == 1:
        namespace = None
        macro_depends = f'macro.dbt.test_{test_name}'
    else:
        assert False, f'invalid test name: {test_name}'

    if path is None:
        path = 'schema.yml'
    if tags is None:
        tags = ['schema']

    if refs is None:
        refs = []
    if sources is None:
        sources = []

    depends_on_nodes = []
    for ref in refs:
        ref_values.append([ref.name])
        depends_on_nodes.append(ref.unique_id)

    for source in sources:
        source_values.append([source.source_name, source.name])
        depends_on_nodes.append(source.unique_id)

    return ParsedGenericTestNode(
        raw_sql=raw_sql,
        test_metadata=TestMetadata(
            namespace=namespace,
            name=test_name,
            kwargs=kwargs,
        ),
        database='dbt',
        schema='dbt_postgres',
        name=node_name,
        alias=node_name,
        fqn=['minimal', 'schema_test', node_name],
        unique_id=f'test.{pkg}.{node_name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=f'schema_test/{node_name}.sql',
        original_file_path=f'models/{path}',
        resource_type=NodeType.Test,
        tags=tags,
        refs=ref_values,
        sources=[],
        depends_on=DependsOn(
            macros=[macro_depends],
            nodes=depends_on_nodes
        ),
        column_name=column_name,
        checksum=FileHash.from_contents(''),
    )


def make_data_test(pkg, name, sql, refs=None, sources=None, tags=None, path=None, config_kwargs=None):

    if refs is None:
        refs = []
    if sources is None:
        sources = []
    if tags is None:
        tags = ['data']
    if path is None:
        path = f'{name}.sql'

    if config_kwargs is None:
        config_kwargs = {}

    fqn = ['minimal', 'data_test', name]

    depends_on_nodes = []
    source_values = []
    ref_values = []
    for ref in refs:
        ref_values.append([ref.name])
        depends_on_nodes.append(ref.unique_id)
    for src in sources:
        source_values.append([src.source_name, src.name])
        depends_on_nodes.append(src.unique_id)

    return ParsedSingularTestNode(
        raw_sql=sql,
        database='dbt',
        schema='dbt_schema',
        name=name,
        alias=name,
        fqn=fqn,
        unique_id=f'test.{pkg}.{name}',
        package_name=pkg,
        root_path='/usr/dbt/some-project',
        path=path,
        original_file_path=f'tests/{path}',
        config=TestConfig(**config_kwargs),
        tags=tags,
        refs=ref_values,
        sources=source_values,
        depends_on=DependsOn(nodes=depends_on_nodes, macros=[]),
        resource_type=NodeType.Test,
        checksum=FileHash.from_contents(''),
    )


def make_exposure(pkg, name, path=None, fqn_extras=None, owner=None):
    if path is None:
        path = 'schema.yml'

    if fqn_extras is None:
        fqn_extras = []

    if owner is None:
        owner = ExposureOwner(email='test@example.com')

    fqn = [pkg, 'exposures'] + fqn_extras + [name]
    return ParsedExposure(
        name=name,
        type=ExposureType.Notebook,
        fqn=fqn,
        unique_id=f'exposure.{pkg}.{name}',
        package_name=pkg,
        path=path,
        root_path='/usr/src/app',
        original_file_path=path,
        owner=owner,
    )


def make_metric(pkg, name, path=None):
    if path is None:
        path = 'schema.yml'

    return ParsedMetric(
        name=name,
        path='schema.yml',
        package_name=pkg,
        root_path='/usr/src/app',
        original_file_path=path,
        unique_id=f'metric.{pkg}.{name}',
        fqn=[pkg, 'metrics', name],
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
           value=True,
           operator="=",
        )],
        meta={'is_okr': True},
        tags=['okrs'],
    )


@pytest.fixture
def macro_test_unique():
    return make_macro(
        'dbt',
        'test_unique',
        'blablabla',
        depends_on_macros=['macro.dbt.default__test_unique']
    )


@pytest.fixture
def macro_default_test_unique():
    return make_macro(
        'dbt',
        'default__test_unique',
        'blablabla'
    )


@pytest.fixture
def macro_test_not_null():
    return make_macro(
        'dbt',
        'test_not_null',
        'blablabla',
        depends_on_macros=['macro.dbt.default__test_not_null']
    )


@pytest.fixture
def macro_default_test_not_null():
    return make_macro(
        'dbt',
        'default__test_not_null',
        'blabla'
    )


@pytest.fixture
def seed():
    return make_seed(
        'pkg',
        'seed'
    )


@pytest.fixture
def source():
    return make_source(
        'pkg',
        'raw',
        'seed',
        identifier='seed'
    )


@pytest.fixture
def ephemeral_model(source):
    return make_model(
        'pkg',
        'ephemeral_model',
        'select * from {{ source("raw", "seed") }}',
        config_kwargs={'materialized': 'ephemeral'},
        sources=[source],
    )


@pytest.fixture
def view_model(ephemeral_model):
    return make_model(
        'pkg',
        'view_model',
        'select * from {{ ref("ephemeral_model") }}',
        config_kwargs={'materialized': 'view'},
        refs=[ephemeral_model],
        tags=['uses_ephemeral'],
    )


@pytest.fixture
def table_model(ephemeral_model):
    return make_model(
        'pkg',
        'table_model',
        'select * from {{ ref("ephemeral_model") }}',
        config_kwargs={'materialized': 'table'},
        refs=[ephemeral_model],
        tags=['uses_ephemeral'],
        path='subdirectory/table_model.sql'
    )


@pytest.fixture
def ext_source():
    return make_source(
        'ext',
        'ext_raw',
        'ext_source',
    )


@pytest.fixture
def ext_source_2():
    return make_source(
        'ext',
        'ext_raw',
        'ext_source_2',
    )


@pytest.fixture
def ext_source_other():
    return make_source(
        'ext',
        'raw',
        'ext_source',
    )


@pytest.fixture
def ext_source_other_2():
    return make_source(
        'ext',
        'raw',
        'ext_source_2',
    )


@pytest.fixture
def ext_model(ext_source):
    return make_model(
        'ext',
        'ext_model',
        'select * from {{ source("ext_raw", "ext_source") }}',
        sources=[ext_source],
    )


@pytest.fixture
def union_model(seed, ext_source):
    return make_model(
        'pkg',
        'union_model',
        'select * from {{ ref("seed") }} union all select * from {{ source("ext_raw", "ext_source") }}',
        config_kwargs={'materialized': 'table'},
        refs=[seed],
        sources=[ext_source],
        fqn_extras=['unions'],
        path='subdirectory/union_model.sql',
        tags=['unions'],
    )


@pytest.fixture
def table_id_unique(table_model):
    return make_unique_test('pkg', table_model, 'id')


@pytest.fixture
def table_id_not_null(table_model):
    return make_not_null_test('pkg', table_model, 'id')


@pytest.fixture
def view_id_unique(view_model):
    return make_unique_test('pkg', view_model, 'id')


@pytest.fixture
def ext_source_id_unique(ext_source):
    return make_unique_test('ext', ext_source, 'id')


@pytest.fixture
def view_test_nothing(view_model):
    return make_data_test('pkg', 'view_test_nothing', 'select * from {{ ref("view_model") }} limit 0', refs=[view_model])

# Support dots as namespace separators
@pytest.fixture
def namespaced_seed():
    return make_seed(
        'pkg',
        'mynamespace.seed'
    )

@pytest.fixture
def namespace_model(source):
    return make_model(
        'pkg',
        'mynamespace.ephemeral_model',
        'select * from {{ source("raw", "seed") }}',
        config_kwargs={'materialized': 'ephemeral'},
        sources=[source],
    )

@pytest.fixture
def namespaced_union_model(seed, ext_source):
    return make_model(
        'pkg',
        'mynamespace.union_model',
        'select * from {{ ref("mynamespace.seed") }} union all select * from {{ ref("mynamespace.ephemeral_model") }}',
        config_kwargs={'materialized': 'table'},
        refs=[seed],
        sources=[ext_source],
        fqn_extras=['unions'],
        path='subdirectory/union_model.sql',
        tags=['unions'],
    )


@pytest.fixture
def manifest(seed, source, ephemeral_model, view_model, table_model, ext_source, ext_model, union_model, ext_source_2, 
    ext_source_other, ext_source_other_2, table_id_unique, table_id_not_null, view_id_unique, ext_source_id_unique, 
    view_test_nothing, namespaced_seed, namespace_model, namespaced_union_model, macro_test_unique, macro_default_test_unique,
    macro_test_not_null, macro_default_test_not_null):
    nodes = [seed, ephemeral_model, view_model, table_model, union_model, ext_model,
             table_id_unique, table_id_not_null, view_id_unique, ext_source_id_unique, view_test_nothing,
             namespaced_seed, namespace_model, namespaced_union_model]
    sources = [source, ext_source, ext_source_2,
               ext_source_other, ext_source_other_2]
    macros = [macro_test_unique, macro_default_test_unique, macro_test_not_null, macro_default_test_not_null]
    manifest = Manifest(
        nodes={n.unique_id: n for n in nodes},
        sources={s.unique_id: s for s in sources},
        macros={m.unique_id: m for m in macros},
        docs={},
        files={},
        exposures={},
        metrics={},
        disabled=[],
        selectors={},
    )
    return manifest


def search_manifest_using_method(manifest, method, selection):
    selected = method.search(set(manifest.nodes) | set(
        manifest.sources) | set(manifest.exposures) | set(manifest.metrics), selection)
    results = {manifest.expect(uid).search_name for uid in selected}
    return results


def test_select_fqn(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('fqn', [])
    assert isinstance(method, QualifiedNameSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(
        manifest, method, 'pkg.unions') == {'union_model', 'mynamespace.union_model'}
    assert not search_manifest_using_method(manifest, method, 'ext.unions')
    # sources don't show up, because selection pretends they have no FQN. Should it?
    assert search_manifest_using_method(manifest, method, 'pkg') == {
        'union_model', 'table_model', 'view_model', 'ephemeral_model', 'seed',
        'mynamespace.union_model', 'mynamespace.ephemeral_model', 'mynamespace.seed'}
    assert search_manifest_using_method(
        manifest, method, 'ext') == {'ext_model'}


def test_select_tag(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('tag', [])
    assert isinstance(method, TagSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'uses_ephemeral') == {
        'view_model', 'table_model'}
    assert not search_manifest_using_method(manifest, method, 'missing')


def test_select_source(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('source', [])
    assert isinstance(method, SourceSelectorMethod)
    assert method.arguments == []

    # the lookup is based on how many components you provide: source, source.table, package.source.table
    assert search_manifest_using_method(manifest, method, 'raw') == {
        'raw.seed', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(
        manifest, method, 'raw.seed') == {'raw.seed'}
    assert search_manifest_using_method(
        manifest, method, 'pkg.raw.seed') == {'raw.seed'}
    assert search_manifest_using_method(
        manifest, method, 'pkg.*.*') == {'raw.seed'}
    assert search_manifest_using_method(
        manifest, method, 'raw.*') == {'raw.seed', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(
        manifest, method, 'ext.raw.*') == {'raw.ext_source', 'raw.ext_source_2'}
    assert not search_manifest_using_method(manifest, method, 'missing')
    assert not search_manifest_using_method(manifest, method, 'raw.missing')
    assert not search_manifest_using_method(
        manifest, method, 'missing.raw.seed')

    assert search_manifest_using_method(manifest, method, 'ext.*.*') == {
        'ext_raw.ext_source', 'ext_raw.ext_source_2', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(manifest, method, 'ext_raw') == {
        'ext_raw.ext_source', 'ext_raw.ext_source_2'}
    assert search_manifest_using_method(
        manifest, method, 'ext.ext_raw.*') == {'ext_raw.ext_source', 'ext_raw.ext_source_2'}
    assert not search_manifest_using_method(manifest, method, 'pkg.ext_raw.*')


# TODO: this requires writing out files
@pytest.mark.skip('TODO: write manifest files to disk')
def test_select_path(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('path', [])
    assert isinstance(method, PathSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(
        manifest, method, 'subdirectory/*.sql') == {'union_model', 'table_model'}
    assert search_manifest_using_method(
        manifest, method, 'subdirectory/union_model.sql') == {'union_model'}
    assert search_manifest_using_method(
        manifest, method, 'models/*.sql') == {'view_model', 'ephemeral_model'}
    assert not search_manifest_using_method(manifest, method, 'missing')
    assert not search_manifest_using_method(
        manifest, method, 'models/missing.sql')
    assert not search_manifest_using_method(
        manifest, method, 'models/missing*')


def test_select_package(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('package', [])
    assert isinstance(method, PackageSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'pkg') == {'union_model', 'table_model', 'view_model', 'ephemeral_model',
                                                                     'seed', 'raw.seed', 'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'view_test_nothing',
                                                                     'mynamespace.seed', 'mynamespace.ephemeral_model', 'mynamespace.union_model',
                                                                     }
    assert search_manifest_using_method(manifest, method, 'ext') == {
        'ext_model', 'ext_raw.ext_source', 'ext_raw.ext_source_2', 'raw.ext_source', 'raw.ext_source_2', 'unique_ext_raw_ext_source_id'}

    assert not search_manifest_using_method(manifest, method, 'missing')


def test_select_config_materialized(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('config', ['materialized'])
    assert isinstance(method, ConfigSelectorMethod)
    assert method.arguments == ['materialized']

    assert search_manifest_using_method(manifest, method, 'view') == {
        'view_model', 'ext_model'}
    assert search_manifest_using_method(manifest, method, 'table') == {
        'table_model', 'union_model', 'mynamespace.union_model'}


def test_select_test_name(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('test_name', [])
    assert isinstance(method, TestNameSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'unique') == {
        'unique_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id'}
    assert search_manifest_using_method(manifest, method, 'not_null') == {
        'not_null_table_model_id'}
    assert not search_manifest_using_method(manifest, method, 'notatest')


def test_select_test_type(manifest):
    methods = MethodManager(manifest, None)
    method = methods.get_method('test_type', [])
    assert isinstance(method, TestTypeSelectorMethod)
    assert method.arguments == []
    assert search_manifest_using_method(manifest, method, 'generic') == {
        'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id'}
    assert search_manifest_using_method(manifest, method, 'singular') == {
        'view_test_nothing'}
    # test backwards compatibility
    assert search_manifest_using_method(manifest, method, 'schema') == {
        'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id'}
    assert search_manifest_using_method(manifest, method, 'data') == {
        'view_test_nothing'}


def test_select_exposure(manifest):
    exposure = make_exposure('test', 'my_exposure')
    manifest.exposures[exposure.unique_id] = exposure
    methods = MethodManager(manifest, None)
    method = methods.get_method('exposure', [])
    assert isinstance(method, ExposureSelectorMethod)
    assert search_manifest_using_method(
        manifest, method, 'my_exposure') == {'my_exposure'}
    assert not search_manifest_using_method(
        manifest, method, 'not_my_exposure')


def test_select_metric(manifest):
    metric = make_metric('test', 'my_metric')
    manifest.metrics[metric.unique_id] = metric
    methods = MethodManager(manifest, None)
    method = methods.get_method('metric', [])
    assert isinstance(method, MetricSelectorMethod)
    assert search_manifest_using_method(
        manifest, method, 'my_metric') == {'my_metric'}
    assert not search_manifest_using_method(
        manifest, method, 'not_my_metric')


@pytest.fixture
def previous_state(manifest):
    writable = copy.deepcopy(manifest).writable_manifest()
    state = PreviousState(Path('/path/does/not/exist'))
    state.manifest = writable
    return state


def add_node(manifest, node):
    manifest.nodes[node.unique_id] = node


def add_macro(manifest, macro):
    manifest.macros[macro.unique_id] = macro


def change_node(manifest, node, change=None):
    if change is not None:
        node = change(node)
    manifest.nodes[node.unique_id] = node


def statemethod(manifest, previous_state):
    methods = MethodManager(manifest, previous_state)
    method = methods.get_method('state', [])
    assert isinstance(method, StateSelectorMethod)
    assert method.arguments == []
    return method


def test_select_state_no_change(manifest, previous_state):
    method = statemethod(manifest, previous_state)
    assert not search_manifest_using_method(manifest, method, 'modified')
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.configs')
    assert not search_manifest_using_method(manifest, method, 'modified.persisted_descriptions')
    assert not search_manifest_using_method(manifest, method, 'modified.relation')
    assert not search_manifest_using_method(manifest, method, 'modified.macros')


def test_select_state_nothing(manifest, previous_state):
    previous_state.manifest = None
    method = statemethod(manifest, previous_state)
    with pytest.raises(dbt.exceptions.RuntimeException) as exc:
        search_manifest_using_method(manifest, method, 'modified')
    assert 'no comparison manifest' in str(exc.value)

    with pytest.raises(dbt.exceptions.RuntimeException) as exc:
        search_manifest_using_method(manifest, method, 'new')
    assert 'no comparison manifest' in str(exc.value)


def test_select_state_added_model(manifest, previous_state):
    add_node(manifest, make_model('pkg', 'another_model', 'select 1 as id'))
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'another_model'}
    assert search_manifest_using_method(
        manifest, method, 'new') == {'another_model'}
    assert search_manifest_using_method(
        manifest, method, 'modified.body') == {'another_model'}


def test_select_state_changed_model_sql(manifest, previous_state, view_model):
    change_node(manifest, view_model.replace(raw_sql='select 1 as id'))
    method = statemethod(manifest, previous_state)
    
    # both of these
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'view_model'}
    assert search_manifest_using_method(
        manifest, method, 'modified.body') == {'view_model'}
    
    # none of these
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.configs')
    assert not search_manifest_using_method(manifest, method, 'modified.persisted_descriptions')
    assert not search_manifest_using_method(manifest, method, 'modified.relation')
    assert not search_manifest_using_method(manifest, method, 'modified.macros')


def test_select_state_changed_model_fqn(manifest, previous_state, view_model):
    change_node(manifest, view_model.replace(
        fqn=view_model.fqn[:-1]+['nested']+view_model.fqn[-1:]))
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'view_model'}
    assert not search_manifest_using_method(manifest, method, 'new')


def test_select_state_added_seed(manifest, previous_state):
    add_node(manifest, make_seed('pkg', 'another_seed'))
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'another_seed'}
    assert search_manifest_using_method(
        manifest, method, 'new') == {'another_seed'}


def test_select_state_changed_seed_checksum_sha_to_sha(manifest, previous_state, seed):
    change_node(manifest, seed.replace(
        checksum=FileHash.from_contents('changed')))
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')


def test_select_state_changed_seed_checksum_path_to_path(manifest, previous_state, seed):
    change_node(previous_state.manifest, seed.replace(
        checksum=FileHash(name='path', checksum=seed.original_file_path)))
    change_node(manifest, seed.replace(checksum=FileHash(
        name='path', checksum=seed.original_file_path)))
    method = statemethod(manifest, previous_state)
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert not search_manifest_using_method(manifest, method, 'modified')
        warn_or_error_patch.assert_called_once()
        msg = warn_or_error_patch.call_args[0][0]
        assert msg.startswith('Found a seed (pkg.seed) >1MB in size')
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert not search_manifest_using_method(manifest, method, 'new')
        warn_or_error_patch.assert_not_called()


def test_select_state_changed_seed_checksum_sha_to_path(manifest, previous_state, seed):
    change_node(manifest, seed.replace(checksum=FileHash(
        name='path', checksum=seed.original_file_path)))
    method = statemethod(manifest, previous_state)
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert search_manifest_using_method(
            manifest, method, 'modified') == {'seed'}
        warn_or_error_patch.assert_called_once()
        msg = warn_or_error_patch.call_args[0][0]
        assert msg.startswith('Found a seed (pkg.seed) >1MB in size')
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert not search_manifest_using_method(manifest, method, 'new')
        warn_or_error_patch.assert_not_called()


def test_select_state_changed_seed_checksum_path_to_sha(manifest, previous_state, seed):
    change_node(previous_state.manifest, seed.replace(
        checksum=FileHash(name='path', checksum=seed.original_file_path)))
    method = statemethod(manifest, previous_state)
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert search_manifest_using_method(
            manifest, method, 'modified') == {'seed'}
        warn_or_error_patch.assert_not_called()
    with mock.patch('dbt.contracts.graph.parsed.warn_or_error') as warn_or_error_patch:
        assert not search_manifest_using_method(manifest, method, 'new')
        warn_or_error_patch.assert_not_called()


def test_select_state_changed_seed_fqn(manifest, previous_state, seed):
    change_node(manifest, seed.replace(
        fqn=seed.fqn[:-1]+['nested']+seed.fqn[-1:]))
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')


def test_select_state_changed_seed_relation_documented(manifest, previous_state, seed):
    seed_doc_relation = replace_config(seed, persist_docs={'relation': True})
    change_node(manifest, seed_doc_relation)
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.configs') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.body')
    assert not search_manifest_using_method(manifest, method, 'modified.persisted_descriptions')


def test_select_state_changed_seed_relation_documented_nodocs(manifest, previous_state, seed):
    seed_doc_relation = replace_config(seed, persist_docs={'relation': True})
    seed_doc_relation_documented = seed_doc_relation.replace(
        description='a description')
    change_node(previous_state.manifest, seed_doc_relation)
    change_node(manifest, seed_doc_relation_documented)
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.persisted_descriptions') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.configs')


def test_select_state_changed_seed_relation_documented_withdocs(manifest, previous_state, seed):
    seed_doc_relation = replace_config(seed, persist_docs={'relation': True})
    seed_doc_relation_documented = seed_doc_relation.replace(
        description='a description')
    change_node(previous_state.manifest, seed_doc_relation_documented)
    change_node(manifest, seed_doc_relation)
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.persisted_descriptions') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')


def test_select_state_changed_seed_columns_documented(manifest, previous_state, seed):
    # changing persist_docs, even without changing the description -> changed
    seed_doc_columns = replace_config(seed, persist_docs={'columns': True})
    change_node(manifest, seed_doc_columns)
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.configs') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.persisted_descriptions')


def test_select_state_changed_seed_columns_documented_nodocs(manifest, previous_state, seed):
    seed_doc_columns = replace_config(seed, persist_docs={'columns': True})
    seed_doc_columns_documented_columns = seed_doc_columns.replace(
        columns={'a': ColumnInfo(name='a', description='a description')},
    )

    change_node(previous_state.manifest, seed_doc_columns)
    change_node(manifest, seed_doc_columns_documented_columns)

    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.persisted_descriptions') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.configs')


def test_select_state_changed_seed_columns_documented_withdocs(manifest, previous_state, seed):
    seed_doc_columns = replace_config(seed, persist_docs={'columns': True})
    seed_doc_columns_documented_columns = seed_doc_columns.replace(
        columns={'a': ColumnInfo(name='a', description='a description')},
    )

    change_node(manifest, seed_doc_columns)
    change_node(previous_state.manifest, seed_doc_columns_documented_columns)

    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'seed'}
    assert search_manifest_using_method(
        manifest, method, 'modified.persisted_descriptions') == {'seed'}
    assert not search_manifest_using_method(manifest, method, 'new')
    assert not search_manifest_using_method(manifest, method, 'modified.configs')
    
def test_select_state_changed_test_macro_sql(manifest, previous_state, macro_default_test_not_null):
    manifest.macros[macro_default_test_not_null.unique_id] = macro_default_test_not_null.replace(macro_sql='lalala')
    method = statemethod(manifest, previous_state)
    assert search_manifest_using_method(
        manifest, method, 'modified') == {'not_null_table_model_id'}
    assert search_manifest_using_method(
        manifest, method, 'modified.macros') == {'not_null_table_model_id'}
    assert not search_manifest_using_method(manifest, method, 'new')

def test_select_state_changed_test_macros(manifest, previous_state):
    changed_macro = make_macro('dbt', 'changed_macro', 'blablabla')
    add_macro(manifest, changed_macro)
    add_macro(previous_state.manifest, changed_macro.replace(macro_sql='something different'))

    unchanged_macro = make_macro('dbt', 'unchanged_macro', 'blablabla')
    add_macro(manifest, unchanged_macro)
    add_macro(previous_state.manifest, unchanged_macro)

    model1 = make_model('dbt', 'model1', 'blablabla',
            depends_on_macros=[changed_macro.unique_id, unchanged_macro.unique_id])
    add_node(manifest, model1)
    add_node(previous_state.manifest, model1)

    model2 = make_model('dbt', 'model2', 'blablabla',
            depends_on_macros=[unchanged_macro.unique_id, changed_macro.unique_id])
    add_node(manifest, model2)
    add_node(previous_state.manifest, model2)

    method = statemethod(manifest, previous_state)

    assert search_manifest_using_method(
        manifest, method, 'modified') == {'model1', 'model2'}
    assert search_manifest_using_method(
        manifest, method, 'modified.macros') == {'model1', 'model2'}
    assert not search_manifest_using_method(manifest, method, 'new')
