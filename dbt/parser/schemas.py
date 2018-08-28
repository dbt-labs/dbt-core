import os
import re
import hashlib

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.yaml_helper
import dbt.context.parser
import dbt.contracts.project

from dbt.node_types import NodeType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path
from dbt.contracts.graph.unparsed import UnparsedNode, UnparsedNodeUpdate
from dbt.contracts.graph.parsed import ParsedNodePatch
from dbt.parser.base import BaseParser


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
    is_function = re.match(r'^\s*(ref|var)\s*\(.+\)\s*$', test_value)

    # if the value is a function, don't wrap it in quotes!
    if is_function:
        formatted_value = value
    else:
        formatted_value = value.__repr__()

    return "{key}={value}".format(key=key, value=formatted_value)


def build_test_raw_sql(test_namespace, model_name, test_type, test_args):
    """Build the raw SQL from a test definition.

    :param test_namespace: The test's namespace, if one exists
    :param model_name: The model name under test
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
            'model': model_name,
            'macro': macro_name,
            'kwargs': ", ".join(kwargs)
        }
    )
    return raw_sql


class SchemaParser(BaseParser):
    """This is the original schema parser but with everything in one huge CF of
    a method so I can refactor it more nicely.
    """
    @staticmethod
    def check_v2_missing_version(path, test_yml):
        """Given the loaded yaml from a file, return True if it looks like the
        file is probably a v2 schema.yml with a missing `version: 2`.
        """
        # in v1, it's Dict[str, dict] instead of Dict[str, list]
        if 'models' in test_yml and isinstance(test_yml['models'], list):
            dbt.exceptions.raise_incorrect_version(path)

    @classmethod
    def _build_v1_test_args(cls, config):
        if isinstance(config, (basestring, int, float, bool)):
            return {'arg': config}
        else:
            return config

    @classmethod
    def _build_v2_test_args(cls, test, name):
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
        if name is not None:
            test_args['column_name'] = name
        return test_name, test_args

    @classmethod
    def calculate_namespace(cls, test_type, package_name):
        test_namespace = None
        split = test_type.split('.')
        if len(split) > 1:
            test_type = split[1]
            package_name = split[0]
            test_namespace = package_name

        return test_namespace, test_type, package_name

    @classmethod
    def build_unparsed_node(cls, model_name, package_name, test_type,
                            test_args, test_namespace, root_dir,
                            original_file_path):
        """Given a model name (for the model under test), a pacakge name,
        a test type (identifying the test macro to use), arguments dictionary,
        the root directory of the search, and the original file path to the
        schema.yml file that specified the test, build an UnparsedNode
        representing the test.
        """
        test_path = os.path.basename(original_file_path)

        raw_sql = build_test_raw_sql(test_namespace, model_name, test_type,
                                     test_args)

        hashed_name, full_name = get_nice_schema_test_name(test_type,
                                                           model_name,
                                                           test_args)

        hashed_path = get_pseudo_test_path(hashed_name, test_path,
                                           'schema_test')
        full_path = get_pseudo_test_path(full_name, test_path,
                                         'schema_test')
        return UnparsedNode(
            name=full_name,
            resource_type=NodeType.Test,
            package_name=package_name,
            root_path=root_dir,
            path=hashed_path,
            original_file_path=original_file_path,
            raw_sql=raw_sql
        )

    @classmethod
    def build_parsed_node(cls, unparsed, model_name, test_namespace, test_type,
                          root_project, all_projects, macros, column_name):
        """Given an UnparsedNode with a node type of Test and some extra
        information, build a ParsedNode representing the test.
        """

        test_path = os.path.basename(unparsed.original_file_path)

        source_package = all_projects.get(unparsed.package_name)
        if source_package is None:
            desc = '"{}" test on model "{}"'.format(test_type,
                                                    model_name)
            dbt.exceptions.raise_dep_not_found(None, desc, test_namespace)

        # supply our own fqn which overrides the hashed version from the path
        full_path = get_pseudo_test_path(unparsed.name, test_path,
                                         'schema_test')
        fqn_override = cls.get_fqn(full_path, source_package)

        node_path = cls.get_path(NodeType.Test, unparsed.package_name,
                                 unparsed.name)

        return cls.parse_node(unparsed,
                              node_path,
                              root_project,
                              source_package,
                              all_projects,
                              tags=['schema'],
                              fqn_extra=None,
                              fqn=fqn_override,
                              macros=macros,
                              column_name=column_name)

    @classmethod
    def build_node(cls, model_name, package_name, test_type, test_args,
                   root_dir, original_file_path, root_project, all_projects,
                   macros, column_name=None):
        """From the various components that are common to both v1 and v2 schema,
        build a ParsedNode representing a test case.
        """
        original_test_type = test_type
        test_namespace, test_type, package_name = cls.calculate_namespace(
            test_type, package_name
        )

        test_namespace, test_type, package_name = cls.calculate_namespace(
            test_type, package_name
        )

        unparsed = cls.build_unparsed_node(model_name, package_name, test_type,
                                           test_args, test_namespace, root_dir,
                                           original_file_path)

        parsed = cls.build_parsed_node(unparsed, model_name, test_namespace,
                                       original_test_type, root_project,
                                       all_projects, macros, column_name)
        return parsed

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

    @classmethod
    def parse_v1_test_yml(cls, original_file_path, test_yml, package_name,
                          root_project, all_projects, root_dir, macros=None):
        """Parse v1 yml contents, yielding parsed nodes.

        A v1 yml file is laid out like this ('variables' written
        bash-curly-brace style):

            ${model_name}:
                constraints:
                    ${constraint_type}:
                        - ${column_1}
                        - ${column_2}
                    ${other_constraint_type}:
                        - ...
            ${other_model_name}:
                constraints:
                    ...
        """
        for model_name, test_spec in test_yml.items():
            # in v1 we can really only have constraints, so not having any is
            # a concern
            no_tests_warning = (
                "* WARNING: No constraints found for model '{}' in file {}\n"
            )
            if not isinstance(test_spec, dict):
                msg = (
                    "Invalid test config given in {} near {} (expected a dict)"
                ).format(original_file_path, test_spec)
                if dbt.flags.STRICT_MODE:
                    dbt.exceptions.raise_compiler_error(msg)
                dbt.utils.compiler_warning(model_name, msg,
                                           resource_type='test')
                continue

            if test_spec is None or test_spec.get('constraints') is None:
                logger.warning(no_tests_warning.format(model_name,
                               original_file_path))
                continue
            constraints = test_spec.get('constraints', {})
            for test_type, configs in constraints.items():
                if configs is None:
                    continue

                if not isinstance(configs, (list, tuple)):
                    dbt.utils.compiler_warning(
                        model_name,
                        "Invalid test config given in {}".format(
                            original_file_path)
                    )
                    continue

                for config in configs:
                    test_args = cls._build_v1_test_args(config)
                    to_add = cls.build_node(
                        model_name, package_name, test_type, test_args,
                        root_dir, original_file_path,
                        root_project, all_projects, macros)
                    if to_add is not None:
                        yield to_add

    @classmethod
    def parse_v2_yml(cls, original_file_path, test_yml, package_name,
                     root_project, all_projects, root_dir, macros):
        """Parse v2 yml contents, yielding both parsed nodes and node patches.

        A v2 yml file is laid out like this ('variables' written
        bash-curly-brace style):

            models:
                - name: ${model_name}
                  description: ${node_description}
                  columns:
                    - name: ${column_1}
                      description: ${column_1_description}
                      tests:
                          - ${constraint_type}
                          - ${other_constraint_type}
                    - name: ${column_2}
                      description: ${column_2_description}
                      tests:
                          - ${constraint_type}: {$keyword_args_dict}
                          ...
                - name: ${other_model_name}
                  ...
        """
        if 'models' not in test_yml:
            # You could legitimately not have any models in your schema.yml, if
            # sources were supported
            return

        for model in test_yml['models']:
            if not isinstance(model, dict):
                msg = (
                    "Invalid test config given in {} near {} (expected a dict)"
                ).format(original_file_path, model)
                if dbt.flags.STRICT_MODE:
                    dbt.exceptions.raise_compiler_error(msg, model)
                dbt.utils.compiler_warning(model, msg)
                continue
            try:
                model = UnparsedNodeUpdate(**model)
            except dbt.exceptions.JSONValidationException as exc:
                # we don't want to fail the full run, but we do want to fail
                # parsing this file
                msg = "Invalid test config given in {}: {}".format(
                        original_file_path, exc.errors_message
                )
                if dbt.flags.STRICT_MODE:
                    dbt.exceptions.raise_compiler_error(msg, model)

                dbt.utils.compiler_warning(model.get('name'), msg)
                continue

            iterator = cls.parse_model(model, package_name, root_dir,
                                       original_file_path, root_project,
                                       all_projects, macros)

            for node_type, node in iterator:
                yield node_type, node

    @classmethod
    def parse_model(cls, model, package_name, root_dir, path, root_project,
                    all_projects, macros):
        """Given an UnparsedNodeUpdate, return column info about the model

            - column info (name and maybe description) as a dict
            - a list of ParsedNodes repreenting tests

        This is only used in parsing the v2 schema.
        """
        model_name = model['name']
        docrefs = []
        column_info = {}
        for column in model.get('columns', []):
            column_name = column['name']
            description = column.get('description', '')
            column_info[column_name] = {
                'name': column_name,
                'description': description,
            }
            context = {
                'doc': dbt.context.parser.docs(model, docrefs, column_name)
            }
            dbt.clients.jinja.get_rendered(description, context)
            for test in column.get('tests', []):
                test_type, test_args = cls._build_v2_test_args(
                    test, column_name
                )
                node = cls.build_node(
                    model_name, package_name, test_type, test_args, root_dir,
                    path, root_project, all_projects, macros, column_name
                )
                yield 'test', node

        for test in model.get('tests', []):
            # table tests don't inject any extra values, model name is
            # available via `model.name`
            test_type, test_args = cls._build_v2_test_args(test, None)
            node = cls.build_node(model_name, package_name, test_type,
                                  test_args, root_dir, path, root_project,
                                  all_projects, macros)
            yield 'test', node

        context = {'doc': dbt.context.parser.docs(model, docrefs)}
        description = model.get('description', '')
        dbt.clients.jinja.get_rendered(description, context)

        patch = ParsedNodePatch(
            name=model_name,
            original_file_path=path,
            description=description,
            columns=column_info,
            docrefs=docrefs
        )
        yield 'patch', patch

    @classmethod
    def load_and_parse(cls, package_name, root_project, all_projects, root_dir,
                       relative_dirs, macros=None):
        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)
        new_tests = {}  # test unique ID -> ParsedNode
        node_patches = {}  # model name -> dict

        iterator = cls.find_schema_yml(package_name, root_dir, relative_dirs)

        for original_file_path, test_yml in iterator:
            version = test_yml.get('version', 1)
            # the version will not be an int if it's a v1 model that has a
            # model named 'version'.
            if version == 1 or not isinstance(version, int):
                cls.check_v2_missing_version(original_file_path, test_yml)
                new_tests.update(
                    (t.get('unique_id'), t)
                    for t in cls.parse_v1_test_yml(
                        original_file_path, test_yml, package_name,
                        root_project, all_projects, root_dir, macros)
                )
            elif version == 2:
                v2_results = cls.parse_v2_yml(
                        original_file_path, test_yml, package_name,
                        root_project, all_projects, root_dir, macros)
                for result_type, node in v2_results:
                    if result_type == 'patch':
                        node_patches[node.name] = node
                    elif result_type == 'test':
                        new_tests[node.unique_id] = node
                    else:
                        raise dbt.exceptions.InternalException(
                            'Got invalid result type {} '.format(result_type)
                        )
            else:
                dbt.exceptions.raise_compiler_error((
                    'Got an invalid schema.yml version {} in {}, only 1 and 2 '
                    'are supported').format(version, original_file_path)
                )

        return new_tests, node_patches
