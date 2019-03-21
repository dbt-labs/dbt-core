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

from dbt.clients.jinja import get_rendered
from dbt.node_types import NodeType
from dbt.compat import basestring, to_string, to_native_string
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


def build_test_raw_sql(test_namespace, model, test_type, test_args):
    """Build the raw SQL from a test definition.

    :param test_namespace: The test's namespace, if one exists
    :param model: The model under test
    :param test_type: The type of the test (unique_id, etc)
    :param test_args: The arguments passed to the test as a list of `key=value`
        strings
    :return: A string of raw sql for the test node.
    """
    # sort the dict so the keys are rendered deterministically (for tests)
    kwargs = [as_kwarg(key, test_args[key]) for key in sorted(test_args)]

    if test_namespace is None:
        macro_name = "test_{}".format(test_type)
    else:
        macro_name = "{}.test_{}".format(test_namespace, test_type)

    raw_sql = "{{{{ {macro}(model=ref('{model}'), {kwargs}) }}}}".format(
        **{
            'model': model['name'],
            'macro': macro_name,
            'kwargs': ", ".join(kwargs)
        }
    )
    return raw_sql


def build_source_test_raw_sql(test_namespace, source, table, test_type,
                              test_args):
    """Build the raw SQL from a source test definition.

    :param test_namespace: The test's namespace, if one exists
    :param source: The source under test.
    :param table: The table under test
    :param test_type: The type of the test (unique_id, etc)
    :param test_args: The arguments passed to the test as a list of `key=value`
        strings
    :return: A string of raw sql for the test node.
    """
    # sort the dict so the keys are rendered deterministically (for tests)
    kwargs = [as_kwarg(key, test_args[key]) for key in sorted(test_args)]

    if test_namespace is None:
        macro_name = "test_{}".format(test_type)
    else:
        macro_name = "{}.test_{}".format(test_namespace, test_type)

    raw_sql = (
        "{{{{ {macro}(model=source('{source}', '{table}'), {kwargs}) }}}}"
        .format(
            source=source['name'],
            table=table['name'],
            macro=macro_name,
            kwargs=", ".join(kwargs))
    )
    return raw_sql


def calculate_test_namespace(test_type, package_name):
    test_namespace = None
    split = test_type.split('.')
    if len(split) > 1:
        test_type = split[1]
        package_name = split[0]
        test_namespace = package_name

    return test_namespace, test_type, package_name


def _build_test_args(test, name):
    if isinstance(test, basestring):
        test_name = test
        test_args = {}
    elif isinstance(test, dict):
        test = list(test.items())
        if len(test) != 1:
            dbt.exceptions.raise_compiler_error(
                'test definition dictionary must have exactly one key, got'
                ' {} instead ({} keys)'.format(test, len(test))
            )
        test_name, test_args = test[0]
    else:
        dbt.exceptions.raise_compiler_error(
            'test must be dict or str, got {} (value {})'.format(
                type(test), test
            )
        )
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
            warn_invalid(filepath, location, value, '- '+exc.msg)
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
                    'in {}: {}'.format(path, exc.msg), None
                )
                continue

    def _build_raw_sql(self, test_namespace, target, test_type, test_args):
        raise NotImplementedError

    def _generate_test_name(self, target, test_type, test_args):
        """Returns a hashed_name, full_name pair."""
        raise NotImplementedError

    @staticmethod
    def _describe_test_target(test_target):
        raise NotImplementedError

    def build_test_node(self, test_target, package_name, test, root_dir, path,
                        column_name=None):
        """Build a test node against the given target (a model or a source).

        :param test_target: An unparsed form of the target.
        """
        test_type, test_args = _build_test_args(test, column_name)

        test_namespace, test_type, package_name = calculate_test_namespace(
            test_type, package_name
        )

        source_package = self.all_projects.get(package_name)
        if source_package is None:
            desc = '"{}" test on {}'.format(
                test_type, self._describe_test_target(test_target)
            )
            dbt.exceptions.raise_dep_not_found(None, desc, test_namespace)

        test_path = os.path.basename(path)

        hashed_name, full_name = self._generate_test_name(test_target,
                                                          test_type,
                                                          test_args)

        hashed_path = get_pseudo_test_path(hashed_name, test_path,
                                           'schema_test')

        full_path = get_pseudo_test_path(full_name, test_path, 'schema_test')
        raw_sql = self._build_raw_sql(test_namespace, test_target, test_type,
                                      test_args)
        unparsed = UnparsedNode(
            name=full_name,
            resource_type=NodeType.Test,
            package_name=package_name,
            root_path=root_dir,
            path=hashed_path,
            original_file_path=path,
            raw_sql=raw_sql
        )

        # supply our own fqn which overrides the hashed version from the path
        # TODO: is this necessary even a little bit for tests?
        fqn_override = self.get_fqn(full_path, source_package)

        node_path = self.get_path(NodeType.Test, unparsed.package_name,
                                  unparsed.name)

        return self.parse_node(unparsed,
                               node_path,
                               source_package,
                               tags=['schema'],
                               fqn_extra=None,
                               fqn=fqn_override,
                               column_name=column_name)


class SchemaModelParser(SchemaBaseTestParser):
    def _build_raw_sql(self, test_namespace, target, test_type, test_args):
        return build_test_raw_sql(test_namespace, target, test_type, test_args)

    def _generate_test_name(self, target, test_type, test_args):
        return get_nice_schema_test_name(test_type, target['name'], test_args)

    @staticmethod
    def _describe_test_target(test_target):
        return 'model "{}"'.format(test_target)

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
                    'in {}: {}'.format(path, exc.msg), test
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
    def __init__(self, root_project_config, all_projects, macro_manifest):
        super(SchemaSourceParser, self).__init__(
            root_project_config=root_project_config,
            all_projects=all_projects,
            macro_manifest=macro_manifest
        )
        self._renderer = ConfigRenderer(self.root_project_config.cli_vars)

    def _build_raw_sql(self, test_namespace, target, test_type, test_args):
        return build_source_test_raw_sql(test_namespace, target['source'],
                                         target['table'], test_type,
                                         test_args)

    def _generate_test_name(self, target, test_type, test_args):
        return get_nice_schema_test_name(
            'source_'+test_type,
            '{}_{}'.format(target['source']['name'], target['table']['name']),
            test_args
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
            resource_type=NodeType.Source
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
