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
from dbt.contracts.graph.unparsed import UnparsedNode
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


class SchemaParser(BaseParser):

    @classmethod
    def parse_schema_test(cls, test_base, model_name, test_config,
                          test_namespace, test_type, root_project_config,
                          package_project_config, all_projects, macros=None):

        if isinstance(test_config, (basestring, int, float, bool)):
            test_args = {'arg': test_config}
        else:
            test_args = test_config

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

        base_path = test_base.get('path')
        hashed_name, full_name = get_nice_schema_test_name(test_type,
                                                           model_name,
                                                           test_args)

        hashed_path = get_pseudo_test_path(hashed_name, base_path,
                                           'schema_test')
        full_path = get_pseudo_test_path(full_name, base_path,
                                         'schema_test')

        # supply our own fqn which overrides the hashed version from the path
        fqn_override = cls.get_fqn(full_path, package_project_config)
        package_name = test_base.get('package_name')
        node_path = cls.get_path(NodeType.Test, package_name, full_name)

        to_return = UnparsedNode(
            name=full_name,
            resource_type=test_base.get('resource_type'),
            package_name=package_name,
            root_path=test_base.get('root_path'),
            path=hashed_path,
            original_file_path=test_base.get('original_file_path'),
            raw_sql=raw_sql
        )

        return cls.parse_node(to_return,
                              node_path,
                              root_project_config,
                              package_project_config,
                              all_projects,
                              tags=['schema'],
                              fqn_extra=None,
                              fqn=fqn_override,
                              macros=macros)

    @classmethod
    def get_parsed_schema_test(cls, test_node, test_type, model_name, config,
                               root_project, projects, macros):

        package_name = test_node.get('package_name')
        test_namespace = None
        original_test_type = test_type
        split = test_type.split('.')

        if len(split) > 1:
            test_type = split[1]
            package_name = split[0]
            test_namespace = package_name

        source_package = projects.get(package_name)
        if source_package is None:
            desc = '"{}" test on model "{}"'.format(original_test_type,
                                                    model_name)
            dbt.exceptions.raise_dep_not_found(test_node, desc, test_namespace)

        return cls.parse_schema_test(
            test_node,
            model_name,
            config,
            test_namespace,
            test_type,
            root_project,
            source_package,
            all_projects=projects,
            macros=macros)

    @classmethod
    def parse_schema_tests(cls, tests, root_project, projects, macros=None):
        to_return = {}

        for test in tests:
            raw_yml = test.get('raw_yml')
            test_name = "{}:{}".format(test.get('package_name'),
                                       test.get('path'))

            try:
                test_yml = dbt.clients.yaml_helper.load_yaml_text(raw_yml)
            except dbt.exceptions.ValidationException as e:
                test_yml = None
                logger.info("Error reading {} - Skipping\n{}".format(
                            test_name, e))

            if test_yml is None:
                continue

            no_tests_warning = ("* WARNING: No constraints found for model"
                                " '{}' in file {}\n")
            for model_name, test_spec in test_yml.items():
                if test_spec is None or test_spec.get('constraints') is None:
                    test_path = test.get('original_file_path', '<unknown>')
                    logger.warning(no_tests_warning.format(model_name,
                                   test_path))
                    continue

                constraints = test_spec.get('constraints', {})
                for test_type, configs in constraints.items():
                    if configs is None:
                        continue

                    if not isinstance(configs, (list, tuple)):

                        dbt.utils.compiler_warning(
                            model_name,
                            "Invalid test config given in {} near {}".format(
                                test.get('path'),
                                configs))
                        continue

                    for config in configs:
                        to_add = cls.get_parsed_schema_test(
                                    test, test_type, model_name, config,
                                    root_project, projects, macros)

                        if to_add is not None:
                            to_return[to_add.get('unique_id')] = to_add

        return to_return

    @classmethod
    def load_and_parse(cls, package_name, root_project, all_projects, root_dir,
                       relative_dirs, macros=None):
        extension = "[!.#~]*.yml"

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = []

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'), strip=False)

            original_file_path = os.path.join(file_match.get('searched_path'),
                                              file_match.get('relative_path'))

            parts = dbt.utils.split_path(file_match.get('relative_path', ''))
            name, _ = os.path.splitext(parts[-1])

            result.append({
                'name': name,
                'root_path': root_dir,
                'resource_type': NodeType.Test,
                'path': file_match.get('relative_path'),
                'original_file_path': original_file_path,
                'package_name': package_name,
                'raw_yml': file_contents
            })

        return cls.parse_schema_tests(result, root_project, all_projects,
                                      macros)
