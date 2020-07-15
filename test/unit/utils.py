"""Unit test utility functions.

Note that all imports should be inside the functions to avoid import/mocking
issues.
"""
import string
import os
from unittest import mock
from unittest import TestCase

import agate
from hologram import ValidationError


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


class Obj:
    which = 'blah'
    single_threaded = False


def mock_connection(name):
    conn = mock.MagicMock()
    conn.name = name
    return conn


def profile_from_dict(profile, profile_name, cli_vars='{}'):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.context.base import generate_base_context
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(generate_base_context(cli_vars))
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, cli_vars='{}'):
    from dbt.context.target import generate_target_context
    from dbt.config import Project
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(generate_target_context(profile, cli_vars))

    project_root = project.pop('project-root', os.getcwd())

    return Project.render_from_dict(
            project_root, project, packages, renderer
        )


def config_from_parts_or_dicts(project, profile, packages=None, cli_vars='{}'):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get('profile')

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = '/dev/null'
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY
    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def inject_adapter(value, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY
    key = value.type()
    FACTORY.adapters[key] = value


class ContractTestCase(TestCase):
    ContractType = None

    def setUp(self):
        self.maxDiff = None
        super().setUp()

    def assert_to_dict(self, obj, dct):
        self.assertEqual(obj.to_dict(), dct)

    def assert_from_dict(self, obj, dct, cls=None):
        if cls is None:
            cls = self.ContractType
        self.assertEqual(cls.from_dict(dct),  obj)

    def assert_symmetric(self, obj, dct, cls=None):
        self.assert_to_dict(obj, dct)
        self.assert_from_dict(obj, dct, cls)

    def assert_fails_validation(self, dct, cls=None):
        if cls is None:
            cls = self.ContractType

        with self.assertRaises(ValidationError):
            cls.from_dict(dct)


def generate_name_macros(package):
    from dbt.contracts.graph.parsed import ParsedMacro
    from dbt.node_types import NodeType
    name_sql = {}
    for component in ('database', 'schema', 'alias'):
        if component == 'alias':
            source = 'node.name'
        else:
            source = f'target.{component}'
        name = f'generate_{component}_name'
        sql = f'{{% macro {name}(value, node) %}} {{% if value %}} {{{{ value }}}} {{% else %}} {{{{ {source} }}}} {{% endif %}} {{% endmacro %}}'
        name_sql[name] = sql

    for name, sql in name_sql.items():
        pm = ParsedMacro(
            name=name,
            resource_type=NodeType.Macro,
            unique_id=f'macro.{package}.{name}',
            package_name=package,
            original_file_path=normalize('macros/macro.sql'),
            root_path='./dbt_modules/root',
            path=normalize('macros/macro.sql'),
            macro_sql=sql,
        )
        yield pm


class TestAdapterConversions(TestCase):
    def _get_tester_for(self, column_type):
        from dbt.clients import agate_helper
        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if type(instance) is column_type:
                return instance

        raise ValueError(f'no tester for {column_type}')

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[:len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table


def MockMacro(package, name='my_macro', **kwargs):
    from dbt.contracts.graph.parsed import ParsedMacro
    from dbt.node_types import NodeType

    mock_kwargs = dict(
        resource_type=NodeType.Macro,
        package_name=package,
        unique_id=f'macro.{package}.{name}',
        original_file_path='/dev/null',
    )

    mock_kwargs.update(kwargs)

    macro = mock.MagicMock(
        spec=ParsedMacro,
        **mock_kwargs
    )
    macro.name = name
    return macro


def MockMaterialization(package, name='my_materialization', adapter_type=None, **kwargs):
    if adapter_type is None:
        adapter_type = 'default'
    kwargs['adapter_type'] = adapter_type
    return MockMacro(package, f'materialization_{name}_{adapter_type}', **kwargs)


def MockGenerateMacro(package, component='some_component', **kwargs):
    name = f'generate_{component}_name'
    return MockMacro(package, name=name, **kwargs)


def MockSource(package, source_name, name, **kwargs):
    from dbt.node_types import NodeType
    from dbt.contracts.graph.parsed import ParsedSourceDefinition
    src = mock.MagicMock(
        __class__=ParsedSourceDefinition,
        resource_type=NodeType.Source,
        source_name=source_name,
        package_name=package,
        unique_id=f'source.{package}.{source_name}.{name}',
        search_name=f'{source_name}.{name}',
        **kwargs
    )
    src.name = name
    return src


def MockNode(package, name, resource_type=None, **kwargs):
    from dbt.node_types import NodeType
    from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSeedNode
    if resource_type is None:
        resource_type = NodeType.Model
    if resource_type == NodeType.Model:
        cls = ParsedModelNode
    elif resource_type == NodeType.Seed:
        cls = ParsedSeedNode
    else:
        raise ValueError(f'I do not know how to handle {resource_type}')
    node = mock.MagicMock(
        __class__=cls,
        resource_type=resource_type,
        package_name=package,
        unique_id=f'{str(resource_type)}.{package}.{name}',
        search_name=name,
        **kwargs
    )
    node.name = name
    return node


def MockDocumentation(package, name, **kwargs):
    from dbt.node_types import NodeType
    from dbt.contracts.graph.parsed import ParsedDocumentation
    doc = mock.MagicMock(
        __class__=ParsedDocumentation,
        resource_type=NodeType.Documentation,
        package_name=package,
        search_name=name,
        unique_id=f'{package}.{name}',
        **kwargs
    )
    doc.name = name
    return doc
