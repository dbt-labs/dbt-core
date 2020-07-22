import pytest

from datetime import datetime

from dbt.contracts.graph.parsed import (
    DependsOn,
    NodeConfig,
    ParsedModelNode,
    ParsedSeedNode,
    ParsedSnapshotNode,
    ParsedDataTestNode,
    ParsedSchemaTestNode,
    ParsedSourceDefinition,
    TestConfig,
    TestMetadata,
)
from dbt.contracts.graph.manifest import Manifest
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
)


def make_model(pkg, name, sql, refs=None, sources=None, tags=None, path=None, alias=None, config_kwargs=None, fqn_extras=None):
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
        depends_on=DependsOn(nodes=depends_on_nodes),
        resource_type=NodeType.Model,
    )


def make_seed(pkg, name, path=None, loader=None, alias=None, tags=None, fqn_extras=None):
    if alias is None:
        alias = name
    if tags is None:
        tags = []
    if path is None:
        path = f'{name}.csv'

    if fqn_extras is None:
        fqn_extras = []

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
        kwargs['model'] = "{{ source('" + test_model.source_name + "', '" + test_model.name + "') }}"
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
    raw_sql = '{{ config(severity="ERROR") }}{{ test_' + test_name + '(**dbt_schema_test_kwargs) }}'
    name_parts = test_name.split('.')

    if len(name_parts) == 2:
        namespace, test_name = name_parts
        macro_depends = f'model.{namespace}.{test_name}'
    elif len(name_parts) == 1:
        namespace = None
        macro_depends = f'model.dbt.{test_name}'
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

    return ParsedSchemaTestNode(
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
            nodes=['model.minimal.view_model']
        ),
        column_name=column_name,
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

    return ParsedDataTestNode(
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
        depends_on=DependsOn(nodes=depends_on_nodes),
        resource_type=NodeType.Test,
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


@pytest.fixture
def manifest(seed, source, ephemeral_model, view_model, table_model, ext_source, ext_model, union_model, ext_source_2, ext_source_other, ext_source_other_2, table_id_unique, table_id_not_null, view_id_unique, ext_source_id_unique, view_test_nothing):
    nodes = [seed, ephemeral_model, view_model, table_model, union_model, ext_model, table_id_unique, table_id_not_null, view_id_unique, ext_source_id_unique, view_test_nothing]
    sources = [source, ext_source, ext_source_2, ext_source_other, ext_source_other_2]
    manifest = Manifest(
        nodes={n.unique_id: n for n in nodes},
        sources={s.unique_id: s for s in sources},
        macros={},
        docs={},
        files={},
        generated_at=datetime.utcnow(),
        disabled=[],
    )
    return manifest


def search_manifest_using_method(manifest, method, selection):
    selected = method.search(set(manifest.nodes) | set(manifest.sources), selection)
    results = {manifest.expect(uid).search_name for uid in selected}
    return results


def test_select_fqn(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('fqn', [])
    assert isinstance(method, QualifiedNameSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'pkg.unions') == {'union_model'}
    assert not search_manifest_using_method(manifest, method, 'ext.unions')
    # sources don't show up, because selection pretends they have no FQN. Should it?
    assert search_manifest_using_method(manifest, method, 'pkg') == {'union_model', 'table_model', 'view_model', 'ephemeral_model', 'seed'}
    assert search_manifest_using_method(manifest, method, 'ext') == {'ext_model'}


def test_select_tag(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('tag', [])
    assert isinstance(method, TagSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'uses_ephemeral') == {'view_model', 'table_model'}
    assert not search_manifest_using_method(manifest, method, 'missing')


def test_select_source(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('source', [])
    assert isinstance(method, SourceSelectorMethod)
    assert method.arguments == []

    # the lookup is based on how many components you provide: source, source.table, package.source.table
    assert search_manifest_using_method(manifest, method, 'raw') == {'raw.seed', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(manifest, method, 'raw.seed') == {'raw.seed'}
    assert search_manifest_using_method(manifest, method, 'pkg.raw.seed') == {'raw.seed'}
    assert search_manifest_using_method(manifest, method, 'pkg.*.*') == {'raw.seed'}
    assert search_manifest_using_method(manifest, method, 'raw.*') == {'raw.seed', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(manifest, method, 'ext.raw.*') == {'raw.ext_source', 'raw.ext_source_2'}
    assert not search_manifest_using_method(manifest, method, 'missing')
    assert not search_manifest_using_method(manifest, method, 'raw.missing')
    assert not search_manifest_using_method(manifest, method, 'missing.raw.seed')

    assert search_manifest_using_method(manifest, method, 'ext.*.*') == {'ext_raw.ext_source', 'ext_raw.ext_source_2', 'raw.ext_source', 'raw.ext_source_2'}
    assert search_manifest_using_method(manifest, method, 'ext_raw') == {'ext_raw.ext_source', 'ext_raw.ext_source_2'}
    assert search_manifest_using_method(manifest, method, 'ext.ext_raw.*') == {'ext_raw.ext_source', 'ext_raw.ext_source_2'}
    assert not search_manifest_using_method(manifest, method, 'pkg.ext_raw.*')


# TODO: this requires writing out files
@pytest.mark.skip('TODO: write manifest files to disk')
def test_select_path(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('path', [])
    assert isinstance(method, PathSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'subdirectory/*.sql') == {'union_model', 'table_model'}
    assert search_manifest_using_method(manifest, method, 'subdirectory/union_model.sql') == {'union_model'}
    assert search_manifest_using_method(manifest, method, 'models/*.sql') == {'view_model', 'ephemeral_model'}
    assert not search_manifest_using_method(manifest, method, 'missing')
    assert not search_manifest_using_method(manifest, method, 'models/missing.sql')
    assert not search_manifest_using_method(manifest, method, 'models/missing*')


def test_select_package(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('package', [])
    assert isinstance(method, PackageSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'pkg') == {'union_model', 'table_model', 'view_model', 'ephemeral_model', 'seed', 'raw.seed', 'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'view_test_nothing'}
    assert search_manifest_using_method(manifest, method, 'ext') == {'ext_model', 'ext_raw.ext_source', 'ext_raw.ext_source_2', 'raw.ext_source', 'raw.ext_source_2', 'unique_ext_raw_ext_source_id'}

    assert not search_manifest_using_method(manifest, method, 'missing')


def test_select_config_materialized(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('config', ['materialized'])
    assert isinstance(method, ConfigSelectorMethod)
    assert method.arguments == ['materialized']

    # yes, technically tests are "views"
    assert search_manifest_using_method(manifest, method, 'view') == {'view_model', 'ext_model', 'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id', 'view_test_nothing'}
    assert search_manifest_using_method(manifest, method, 'table') == {'table_model', 'union_model'}


def test_select_test_name(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('test_name', [])
    assert isinstance(method, TestNameSelectorMethod)
    assert method.arguments == []

    assert search_manifest_using_method(manifest, method, 'unique') == {'unique_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id'}
    assert search_manifest_using_method(manifest, method, 'not_null') == {'not_null_table_model_id'}
    assert not search_manifest_using_method(manifest, method, 'notatest')


def test_select_test_type(manifest):
    methods = MethodManager(manifest)
    method = methods.get_method('test_type', [])
    assert isinstance(method, TestTypeSelectorMethod)
    assert method.arguments == []
    assert search_manifest_using_method(manifest, method, 'schema') == {'unique_table_model_id', 'not_null_table_model_id', 'unique_view_model_id', 'unique_ext_raw_ext_source_id'}
    assert search_manifest_using_method(manifest, method, 'data') == {'view_test_nothing'}

