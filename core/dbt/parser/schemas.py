from __future__ import unicode_literals
import itertools
import os
import re
import hashlib

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.context.common import generate_config_context
from dbt.clients.jinja import get_rendered
from dbt.node_types import NodeType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path
from dbt.contracts.graph.unparsed import UnparsedNode, UnparsedNodeUpdate, \
    UnparsedSourceDefinition
from dbt.contracts.graph.parsed import ParsedNodePatch, ParsedSourceDefinition
from dbt.parser.base import MacrosKnownParser
from dbt.config.renderer import ConfigRenderer


def get_nice_schema_test_name(test_type, test_name, args):
    flat_args = []
    for arg_name in sorted(args):
        arg_val = args[arg_name]

        if isinstance(arg_val, dict):
            parts = arg_val.values()
        elif isinstance(arg_val, (list, tuple)):
            parts = arg_val
        else:
            parts = [arg_val]

        flat_args.extend([str(part) for part in parts])

    clean_flat_args = [re.sub('[^0-9a-zA-Z_]+', '_', arg) for arg in flat_args]
    unique = "__".join(clean_flat_args)

    cutoff = 32
    if len(unique) <= cutoff:
        label = unique
    else:
        label = hashlib.md5(unique.encode('utf-8')).hexdigest()

    filename = '{}_{}_{}'.format(test_type, test_name, label)
    name = '{}_{}_{}'.format(test_type, test_name, unique)

    return filename, name


def as_kwarg(key, value):
    test_value = to_string(value)
    is_function = re.match(r'^\s*(env_var|ref|var|source|doc)\s*\(.+\)\s*$',
                           test_value)

    # if the value is a function, don't wrap it in quotes!
    if is_function:
        formatted_value = value
    else:
        formatted_value = value.__repr__()

    return "{key}={value}".format(key=key, value=formatted_value)


class TestBuilder(object):
    """An object to hold assorted test settings and perform basic parsing

    Test names have the following pattern:
        - the test name itself may be namespaced (package.test)
        - or it may not be namespaced (test)
        - the test may have arguments embedded in the name (, severity=WARN)
        - or it may not have arguments.

    """
    TEST_NAME_PATTERN = re.compile(
        r'((?P<test_namespace>([a-zA-Z_][0-9a-zA-Z_]*))\.)?'
        r'(?P<test_name>([a-zA-Z_][0-9a-zA-Z_]*))'
    )
    # map magic keys to default values
    MODIFIER_ARGS = {'severity': 'ERROR'}

    def __init__(self, test, target, column_name, package_name, render_ctx):
        test_name, test_args = self.extract_test_args(test, column_name)
        self.args = test_args
        self.package_name = package_name
        self.target = target

        match = self.TEST_NAME_PATTERN.match(test_name)
        if match is None:
            dbt.exceptions.raise_compiler_error(
                'Test name string did not match expected pattern: {}'
                .format(test_name)
            )

        groups = match.groupdict()
        self.name = groups['test_name']
        self.namespace = groups['test_namespace']
        self.modifiers = {}
        for key, default in self.MODIFIER_ARGS.items():
            value = self.args.pop(key, default)
            if isinstance(value, basestring):
                value = get_rendered(value, render_ctx)
            self.modifiers[key] = value

        if self.namespace is not None:
            self.package_name = self.namespace

    @staticmethod
    def extract_test_args(test, name=None):
        if not isinstance(test, dict):
            dbt.exceptions.raise_compiler_error(
                'test must be dict or str, got {} (value {})'.format(
                    type(test), test
                )
            )

        test = list(test.items())
        if len(test) != 1:
            dbt.exceptions.raise_compiler_error(
                'test definition dictionary must have exactly one key, got'
                ' {} instead ({} keys)'.format(test, len(test))
            )
        test_name, test_args = test[0]

        if not isinstance(test_args, dict):
            dbt.exceptions.raise_compiler_error(
                'test arguments must be dict, got {} (value {})'.format(
                    type(test_args), test_args
                )
            )
        if not isinstance(test_name, basestring):
            dbt.exceptions.raise_compiler_error(
                'test name must be a str, got {} (value {})'.format(
                    type(test_name), test_name
                )
            )
        if name is not None:
            test_args['column_name'] = name
        return test_name, test_args

    def severity(self):
        return self.modifiers.get('severity', 'ERROR').upper()

    def test_kwargs_str(self):
        # sort the dict so the keys are rendered deterministically (for tests)
        return ', '.join((
            as_kwarg(key, self.args[key])
            for key in sorted(self.args)
        ))

    def macro_name(self):
        macro_name = 'test_{}'.format(self.name)
        if self.namespace is not None:
            macro_name = "{}.{}".format(self.namespace, macro_name)
        return macro_name

    def build_model_str(self):
        raise NotImplementedError('build_model_str not implemented!')

    def get_test_name(self):
        raise NotImplementedError('get_test_name not implemented!')

    def build_raw_sql(self):
        return (
            "{{{{ config(severity='{severity}') }}}}"
            "{{{{ {macro}(model={model}, {kwargs}) }}}}"
        ).format(
            model=self.build_model_str(),
            macro=self.macro_name(),
            kwargs=self.test_kwargs_str(),
            severity=self.severity()
        )


class RefTestBuilder(TestBuilder):
    def build_model_str(self):
        return "ref('{}')".format(self.target['name'])

    def get_test_name(self):
        return get_nice_schema_test_name(self.name,
                                         self.target['name'],
                                         self.args)

    def describe_test_target(self):
        return 'model "{}"'.format(self.target)


class SourceTestBuilder(TestBuilder):
    def build_model_str(self):
        return "source('{}', '{}')".format(
            self.target['source']['name'],
            self.target['table']['name']
        )

    def get_test_name(self):
        target_name = '{}_{}'.format(self.target['source']['name'],
                                     self.target['table']['name'])
        return get_nice_schema_test_name(
            'source_' + self.name,
            target_name,
            self.args
        )

    def describe_test_target(self):
        return 'source "{0[source]}.{0[table]}"'.format(self.target)


def warn_invalid(filepath, key, value, explain):
    msg = (
        "Invalid test config given in {} @ {}: {} {}"
    ).format(filepath, key, value, explain)
    dbt.exceptions.warn_or_error(msg, value,
                                 log_fmt='Compilation warning: {}\n')


def _filter_validate(filepath, location, values, validate):
    """Generator for validate() results called against all given values. On
    errors, fields are warned about and ignored, unless strict mode is set in
    which case a compiler error is raised.
    """
    for value in values:
        if not isinstance(value, dict):
            warn_invalid(filepath, location, value, '(expected a dict)')
            continue
        try:
            yield validate(**value)
        except dbt.exceptions.JSONValidationException as exc:
            # we don't want to fail the full run, but we do want to fail
            # parsing this file
            warn_invalid(filepath, location, value, '- ' + exc.msg)
            continue


class ParserRef(object):
    """A helper object to hold parse-time references."""
    def __init__(self):
        self.column_info = {}
        self.docrefs = []

    def add(self, column_name, description):
        self.column_info[column_name] = {
            'name': column_name,
            'description': description,
        }


class SchemaBaseTestParser(MacrosKnownParser):
    Builder = TestBuilder

    def _parse_column(self, target, column, package_name, root_dir, path,
                      refs):
        # this should yield ParsedNodes where resource_type == NodeType.Test
        column_name = column['name']
        description = column.get('description', '')

        refs.add(column_name, description)
        context = {
            'doc': dbt.context.parser.docs(target, refs.docrefs, column_name)
        }
        get_rendered(description, context)

        for test in column.get('tests', []):
            try:
                yield self.build_test_node(
                    target, package_name, test, root_dir,
                    path, column_name
                )
            except dbt.exceptions.CompilationException as exc:
                dbt.exceptions.warn_or_error(
                    'Compilation warning: Invalid test config given in {}:'
                    '\n\t{}'.format(path, exc.msg), None
                )
                continue

    def build_test_node(self, test_target, package_name, test, root_dir, path,
                        column_name=None):
        """Build a test node against the given target (a model or a source).

        :param test_target: An unparsed form of the target.
        """
        if isinstance(test, basestring):
            test = {test: {}}

        ctx = generate_config_context(self.root_project_config.cli_vars)

        test_info = self.Builder(test, test_target, column_name, package_name,
                                 ctx)

        source_package = self.all_projects.get(test_info.package_name)
        if source_package is None:
            desc = '"{}" test on {}'.format(
                test_info.name, test_info.describe_test_target()
            )
            dbt.exceptions.raise_dep_not_found(None, desc, test_info.namespace)

        test_path = os.path.basename(path)

        hashed_name, full_name = test_info.get_test_name()

        hashed_path = get_pseudo_test_path(hashed_name, test_path,
                                           'schema_test')

        full_path = get_pseudo_test_path(full_name, test_path, 'schema_test')
        raw_sql = test_info.build_raw_sql()

        unparsed = UnparsedNode(
            name=full_name,
            resource_type=NodeType.Test,
            package_name=test_info.package_name,
            root_path=root_dir,
            path=hashed_path,
            original_file_path=path,
            raw_sql=raw_sql
        )

        # supply our own fqn which overrides the hashed version from the path
        # TODO: is this necessary even a little bit for tests?
        fqn_override = self.get_fqn(unparsed.incorporate(path=full_path),
                                    source_package)

        node_path = self.get_path(NodeType.Test, unparsed.package_name,
                                  unparsed.name)

        result = self.parse_node(unparsed,
                                 node_path,
                                 source_package,
                                 tags=['schema'],
                                 fqn_extra=None,
                                 fqn=fqn_override,
                                 column_name=column_name)

        parse_ok = self.check_block_parsing(full_name, test_path, raw_sql)
        if not parse_ok:
            # if we had a parse error in parse_node, we would not get here. So
            # this means we rejected a good file :(
            raise dbt.exceptions.InternalException(
                'the block parser rejected a good node: {} was marked invalid '
                'but is actually valid!'.format(test_path)
            )
        return result


class SchemaModelParser(SchemaBaseTestParser):
    Builder = RefTestBuilder

    def parse_models_entry(self, model_dict, path, package_name, root_dir):
        model_name = model_dict['name']
        refs = ParserRef()
        for column in model_dict.get('columns', []):
            column_tests = self._parse_column(model_dict, column, package_name,
                                              root_dir, path, refs)
            for node in column_tests:
                yield 'test', node

        for test in model_dict.get('tests', []):
            try:
                node = self.build_test_node(model_dict, package_name, test,
                                            root_dir, path)
            except dbt.exceptions.CompilationException as exc:
                dbt.exceptions.warn_or_error(
                    'Compilation warning: Invalid test config given in {}:'
                    '\n\t{}'.format(path, exc.msg), None
                )
                continue
            yield 'test', node

        context = {'doc': dbt.context.parser.docs(model_dict, refs.docrefs)}
        description = model_dict.get('description', '')
        get_rendered(description, context)

        patch = ParsedNodePatch(
            name=model_name,
            original_file_path=path,
            description=description,
            columns=refs.column_info,
            docrefs=refs.docrefs
        )
        yield 'patch', patch

    def parse_all(self, models, path, package_name, root_dir):
        """Parse all the model dictionaries in models.

        :param List[dict] models: The `models` section of the schema.yml, as a
            list of dicts.
        :param str path: The path to the schema.yml file
        :param str package_name: The name of the current package
        :param str root_dir: The root directory of the search
        """
        filtered = _filter_validate(path, 'models', models, UnparsedNodeUpdate)
        nodes = itertools.chain.from_iterable(
            self.parse_models_entry(model, path, package_name, root_dir)
            for model in filtered
        )
        for node_type, node in nodes:
            yield node_type, node


class SchemaSourceParser(SchemaBaseTestParser):
    Builder = SourceTestBuilder

    def __init__(self, root_project_config, all_projects, macro_manifest):
        super(SchemaSourceParser, self).__init__(
            root_project_config=root_project_config,
            all_projects=all_projects,
            macro_manifest=macro_manifest
        )
        self._renderer = ConfigRenderer(self.root_project_config.cli_vars)

    def _build_raw_sql(self, test_info):
        return test_info.build_source_test_raw_sql()

    def _generate_test_name(self, test_info):
        target_name = '{}_{}'.format(test_info.target['source']['name'],
                                     test_info.target['table']['name'])
        return get_nice_schema_test_name(
            'source_' + test_info.name,
            target_name,
            test_info.args
        )

    @staticmethod
    def _describe_test_target(test_target):
        return 'source "{0[source]}.{0[table]}"'.format(test_target)

    def get_path(self, *parts):
        return '.'.join(str(s) for s in parts)

    def generate_source_node(self, source, table, path, package_name, root_dir,
                             refs):
        unique_id = self.get_path(NodeType.Source, package_name,
                                  source.name, table.name)

        context = {'doc': dbt.context.parser.docs(source, refs.docrefs)}
        description = table.get('description', '')
        source_description = source.get('description', '')
        get_rendered(description, context)
        get_rendered(source_description, context)

        freshness = dbt.utils.deep_merge(source.get('freshness', {}),
                                         table.get('freshness', {}))

        loaded_at_field = table.get('loaded_at_field',
                                    source.get('loaded_at_field'))

        # use 'or {}' to allow quoting: null
        source_quoting = source.get('quoting') or {}
        table_quoting = table.get('quoting') or {}
        quoting = dbt.utils.deep_merge(source_quoting, table_quoting)

        default_database = self.root_project_config.credentials.database
        return ParsedSourceDefinition(
            package_name=package_name,
            database=source.get('database', default_database),
            schema=source.get('schema', source.name),
            identifier=table.get('identifier', table.name),
            root_path=root_dir,
            path=path,
            original_file_path=path,
            columns=refs.column_info,
            unique_id=unique_id,
            name=table.name,
            description=description,
            source_name=source.name,
            source_description=source_description,
            loader=source.get('loader', ''),
            docrefs=refs.docrefs,
            loaded_at_field=loaded_at_field,
            freshness=freshness,
            quoting=quoting,
            resource_type=NodeType.Source,
            fqn=[package_name, source.name, table.name]
        )

    def parse_source_table(self, source, table, path, package_name, root_dir):
        refs = ParserRef()
        test_target = {'source': source, 'table': table}
        for column in table.get('columns', []):
            column_tests = self._parse_column(test_target, column,
                                              package_name, root_dir, path,
                                              refs)
            for node in column_tests:
                yield 'test', node

        for test in table.get('tests', []):
            try:
                node = self.build_test_node(test_target, package_name, test,
                                            root_dir, path)
            except dbt.exceptions.CompilationException as exc:
                dbt.exceptions.warn_or_error(
                    'in {}: {}'.format(path, exc.msg), test
                )
                continue
            yield 'test', node

        node = self.generate_source_node(source, table, path, package_name,
                                         root_dir, refs)
        yield 'source', node

    def parse_source_entry(self, source, path, package_name, root_dir):
        nodes = itertools.chain.from_iterable(
            self.parse_source_table(source, table, path, package_name,
                                    root_dir)
            for table in source.tables
        )
        for node_type, node in nodes:
            yield node_type, node

    def _sources_validate(self, **kwargs):
        kwargs = self._renderer.render_schema_source(kwargs)
        return UnparsedSourceDefinition(**kwargs)

    def parse_all(self, sources, path, package_name, root_dir):
        """Parse all the model dictionaries in sources.

        :param List[dict] sources: The `sources` section of the schema.yml, as
            a list of dicts.
        :param str path: The path to the schema.yml file
        :param str package_name: The name of the current package
        :param str root_dir: The root directory of the search
        """
        filtered = _filter_validate(path, 'sources', sources,
                                    self._sources_validate)
        nodes = itertools.chain.from_iterable(
            self.parse_source_entry(source, path, package_name, root_dir)
            for source in filtered
        )

        for node_type, node in nodes:
            yield node_type, node


class SchemaParser(object):
    def __init__(self, root_project_config, all_projects, macro_manifest):
        self.root_project_config = root_project_config
        self.all_projects = all_projects
        self.macro_manifest = macro_manifest

    @classmethod
    def find_schema_yml(cls, package_name, root_dir, relative_dirs):
        """This is common to both v1 and v2 - look through the relative_dirs
        under root_dir for .yml files yield pairs of filepath and loaded yaml
        contents.
        """
        extension = "[!.#~]*.yml"

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'), strip=False)
            test_path = file_match.get('relative_path', '')

            original_file_path = os.path.join(file_match.get('searched_path'),
                                              test_path)

            try:
                test_yml = dbt.clients.yaml_helper.load_yaml_text(
                    file_contents
                )
            except dbt.exceptions.ValidationException as e:
                test_yml = None
                logger.info("Error reading {}:{} - Skipping\n{}".format(
                            package_name, test_path, e))

            if test_yml is None:
                continue

            yield original_file_path, test_yml

    def parse_schema(self, path, test_yml, package_name, root_dir):
        model_parser = SchemaModelParser(self.root_project_config,
                                         self.all_projects,
                                         self.macro_manifest)
        source_parser = SchemaSourceParser(self.root_project_config,
                                           self.all_projects,
                                           self.macro_manifest)
        models = test_yml.get('models', [])
        sources = test_yml.get('sources', [])
        return itertools.chain(
            model_parser.parse_all(models, path, package_name, root_dir),
            source_parser.parse_all(sources, path, package_name, root_dir),
        )

    def _parse_format_version(self, path, test_yml):
        if 'version' not in test_yml:
            dbt.exceptions.raise_invalid_schema_yml_version(
                path, 'no version is specified'
            )

        version = test_yml['version']
        # if it's not an integer, the version is malformed, or not
        # set. Either way, only 'version: 2' is supported.
        if not isinstance(version, int):
            dbt.exceptions.raise_invalid_schema_yml_version(
                path, 'the version is not an integer'
            )
        return version

    def load_and_parse(self, package_name, root_dir, relative_dirs):
        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**self.all_projects)
        new_tests = {}  # test unique ID -> ParsedNode
        node_patches = {}  # model name -> dict
        new_sources = {}  # source unique ID -> ParsedSourceDefinition

        iterator = self.find_schema_yml(package_name, root_dir, relative_dirs)

        for path, test_yml in iterator:
            version = self._parse_format_version(path, test_yml)
            if version != 2:
                dbt.exceptions.raise_invalid_schema_yml_version(
                    path,
                    'version {} is not supported'.format(version)
                )

            results = self.parse_schema(path, test_yml, package_name, root_dir)
            for result_type, node in results:
                if result_type == 'patch':
                    node_patches[node.name] = node
                elif result_type == 'test':
                    new_tests[node.unique_id] = node
                elif result_type == 'source':
                    new_sources[node.unique_id] = node
                else:
                    raise dbt.exceptions.InternalException(
                        'Got invalid result type {} '.format(result_type)
                    )

        return new_tests, node_patches, new_sources
