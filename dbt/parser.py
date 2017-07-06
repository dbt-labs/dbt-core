import copy
import os
import re

import dbt.flags
import dbt.model
import dbt.utils

import jinja2.runtime
import dbt.clients.jinja
import dbt.clients.yaml_helper

import dbt.contracts.graph.parsed
import dbt.contracts.graph.unparsed
import dbt.contracts.project

from dbt.utils import Var
from dbt.node_types import NodeType, RunHookType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger


def get_path(resource_type, package_name, resource_name):
    return "{}.{}.{}".format(resource_type, package_name, resource_name)


def get_model_path(package_name, resource_name):
    return get_path(NodeType.Model, package_name, resource_name)


def get_test_path(package_name, resource_name):
    return get_path(NodeType.Test, package_name, resource_name)


def get_macro_path(package_name, resource_name):
    return get_path('macros', package_name, resource_name)


def __config(model, cfg):

    def config(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.utils.compiler_error(
                model.get('name'),
                "Invalid model config given inline in {}".format(model))

        cfg.update_in_model_config(opts)

    return config


def __ref(model):

    def ref(*args):
        if len(args) == 1 or len(args) == 2:
            model['refs'].append(args)

        else:
            dbt.exceptions.ref_invalid_args(model, args)

    return ref


def resolve_ref(flat_graph, target_model_name, target_model_package,
                current_project, node_package):

    if target_model_package is not None:
        return dbt.utils.find_model_by_name(
            flat_graph,
            target_model_name,
            target_model_package)

    target_model = None

    # first pass: look for models in the current_project
    target_model = dbt.utils.find_model_by_name(
        flat_graph,
        target_model_name,
        current_project)

    if target_model is not None and dbt.utils.is_enabled(target_model):
        return target_model

    # second pass: look for models in the node's package
    target_model = dbt.utils.find_model_by_name(
        flat_graph,
        target_model_name,
        node_package)

    if target_model is not None and dbt.utils.is_enabled(target_model):
        return target_model

    # final pass: look for models in any package
    # todo: exclude the packages we have already searched. overriding
    # a package model in another package doesn't necessarily work atm
    return dbt.utils.find_model_by_name(
        flat_graph,
        target_model_name,
        None)


def process_refs(flat_graph, current_project):
    for _, node in flat_graph.get('nodes').items():
        target_model = None
        target_model_name = None
        target_model_package = None

        for ref in node.get('refs', []):
            if len(ref) == 1:
                target_model_name = ref[0]
            elif len(ref) == 2:
                target_model_package, target_model_name = ref

            target_model = resolve_ref(
                flat_graph,
                target_model_name,
                target_model_package,
                current_project,
                node.get('package_name'))

            if target_model is None:
                dbt.exceptions.ref_target_not_found(
                    node,
                    target_model_name,
                    target_model_package)

            if (dbt.utils.is_enabled(node) and not
                    dbt.utils.is_enabled(target_model)):
                if dbt.utils.is_type(node, NodeType.Model):
                    dbt.exceptions.ref_disabled_dependency(node, target_model)
                else:
                    node.get('config', {})['enabled'] = False

            target_model_id = target_model.get('unique_id')

            node['depends_on']['nodes'].append(target_model_id)
            flat_graph['nodes'][node['unique_id']] = node

    return flat_graph


def get_fqn(path, package_project_config, extra=[]):
    parts = dbt.utils.split_path(path)
    name, _ = os.path.splitext(parts[-1])
    fqn = ([package_project_config.get('name')] +
           parts[:-1] +
           extra +
           [name])

    return fqn


def parse_macro_file(macro_file_path,
                     macro_file_contents,
                     root_path,
                     package_name,
                     tags=None,
                     context=None):

    logger.debug("Parsing {}".format(macro_file_path))

    to_return = {}

    if tags is None:
        tags = set()

    context = {}

    base_node = {
        'name': 'macro',
        'path': macro_file_path,
        'resource_type': NodeType.Macro,
        'package_name': package_name,
        'depends_on': {
            'macros': [],
        }
    }

    template = dbt.clients.jinja.get_template(
        macro_file_contents, context, node=base_node, validate_macro=True)

    for key, item in template.module.__dict__.items():
        if type(item) == jinja2.runtime.Macro:
            unique_id = get_path(NodeType.Macro,
                                 package_name,
                                 key)

            new_node = base_node.copy()
            new_node.update({
                'name': key,
                'unique_id': unique_id,
                'tags': tags,
                'root_path': root_path,
                'path': macro_file_path,
                'raw_sql': macro_file_contents,
                'parsed_macro': item
            })
            to_return[unique_id] = new_node

    return to_return


def parse_node(node, node_path, root_project_config, package_project_config,
               all_projects, tags=None, fqn_extra=None):
    logger.debug("Parsing {}".format(node_path))
    node = copy.deepcopy(node)

    if tags is None:
        tags = set()

    if fqn_extra is None:
        fqn_extra = []

    node.update({
        'refs': [],
        'depends_on': {
            'nodes': [],
            'macros': [],
        }
    })

    fqn = get_fqn(node.get('path'), package_project_config, fqn_extra)

    config = dbt.model.SourceConfig(
        root_project_config, package_project_config, fqn)

    node['unique_id'] = node_path
    node['empty'] = (len(node.get('raw_sql').strip()) == 0)
    node['fqn'] = fqn
    node['tags'] = tags

    # Set this temporarily. Not the full config yet (as config() hasn't been
    # called from jinja yet). But the Var() call below needs info about project
    # level configs b/c they might contain refs. TODO: Restructure this?
    config_dict = node.get('config', {})
    config_dict.update(config.config)
    node['config'] = config_dict

    context = {}
    context['ref'] = __ref(node)
    context['config'] = __config(node, config)
    context['target'] = property(lambda x: '', lambda x: x)
    context['this'] = ''

    context['var'] = Var(node, context)

    dbt.clients.jinja.get_rendered(
        node.get('raw_sql'), context, node,
        capture_macros=True)

    # Overwrite node config
    config_dict = node.get('config', {})
    config_dict.update(config.config)
    node['config'] = config_dict

    return node


def parse_sql_nodes(nodes, root_project, projects, tags=None):
    if tags is None:
        tags = set()

    to_return = {}

    dbt.contracts.graph.unparsed.validate_nodes(nodes)

    for node in nodes:
        package_name = node.get('package_name')

        node_path = get_path(node.get('resource_type'),
                             package_name,
                             node.get('name'))

        # TODO if this is set, raise a compiler error
        to_return[node_path] = parse_node(node,
                                          node_path,
                                          root_project,
                                          projects.get(package_name),
                                          projects,
                                          tags=tags)

    dbt.contracts.graph.parsed.validate_nodes(to_return)

    return to_return


def load_and_parse_sql(package_name, root_project, all_projects, root_dir,
                       relative_dirs, resource_type, tags=None):
    extension = "[!.#~]*.sql"

    if tags is None:
        tags = set()

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = []

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        parts = dbt.utils.split_path(file_match.get('relative_path', ''))
        name, _ = os.path.splitext(parts[-1])

        if resource_type == NodeType.Test:
            path = dbt.utils.get_pseudo_test_path(
                name, file_match.get('relative_path'), 'data_test')
        elif resource_type == NodeType.Analysis:
            path = os.path.join('analysis', file_match.get('relative_path'))
        else:
            path = file_match.get('relative_path')

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': resource_type,
            'path': path,
            'package_name': package_name,
            'raw_sql': file_contents
        })

    return parse_sql_nodes(result, root_project, all_projects, tags)


def get_hooks_from_project(project_cfg, hook_type):
    hooks = project_cfg.get(hook_type, [])

    if type(hooks) not in (list, tuple):
        hooks = [hooks]

    return hooks


def get_hooks(all_projects, hook_type):
    project_hooks = {}

    for project_name, project in all_projects.items():
        hooks = get_hooks_from_project(project, hook_type)

        if len(hooks) > 0:
            project_hooks[project_name] = ";\n".join(hooks)

    return project_hooks


def load_and_parse_run_hook_type(root_project, all_projects, hook_type):

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    project_hooks = get_hooks(all_projects, hook_type)

    result = []
    for project_name, hooks in project_hooks.items():
        project = all_projects[project_name]

        hook_path = dbt.utils.get_pseudo_hook_path(hook_type)

        result.append({
            'name': hook_type,
            'root_path': "{}/dbt_project.yml".format(project_name),
            'resource_type': NodeType.Operation,
            'path': hook_path,
            'package_name': project_name,
            'raw_sql': hooks
        })

    tags = {hook_type}
    return parse_sql_nodes(result, root_project, all_projects, tags=tags)


def load_and_parse_run_hooks(root_project, all_projects):
    hook_nodes = {}
    for hook_type in RunHookType.Both:
        project_hooks = load_and_parse_run_hook_type(root_project,
                                                     all_projects,
                                                     hook_type)
        hook_nodes.update(project_hooks)

    return hook_nodes


def load_and_parse_macros(package_name, root_project, all_projects, root_dir,
                          relative_dirs, resource_type, tags=None):
    extension = "[!.#~]*.sql"

    if tags is None:
        tags = set()

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = {}

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        result.update(
            parse_macro_file(
                file_match.get('relative_path'),
                file_contents,
                root_dir,
                package_name))

    dbt.contracts.graph.parsed.validate_macros(result)

    return result


def parse_schema_tests(tests, root_project, projects):
    to_return = {}

    for test in tests:
        raw_yml = test.get('raw_yml')
        test_name = "{}:{}".format(test.get('package_name'), test.get('path'))

        try:
            test_yml = dbt.clients.yaml_helper.load_yaml_text(raw_yml)
        except dbt.exceptions.ValidationException as e:
            test_yml = None
            logger.info("Error reading {} - Skipping\n{}".format(test_name, e))

        if test_yml is None:
            continue

        for model_name, test_spec in test_yml.items():
            if test_spec is None or test_spec.get('constraints') is None:
                continue

            for test_type, configs in test_spec.get('constraints', {}).items():
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
                    to_add = parse_schema_test(
                        test, model_name, config, test_type,
                        root_project,
                        projects.get(test.get('package_name')),
                        all_projects=projects)

                    if to_add is not None:
                        to_return[to_add.get('unique_id')] = to_add

    return to_return


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
    return '{}_{}_{}'.format(test_type, test_name, unique)


def as_kwarg(key, value):
    test_value = to_string(value)
    is_function = re.match(r'^\s*(ref|var)\s*\(.+\)\s*$', test_value)

    # if the value is a function, don't wrap it in quotes!
    if is_function:
        formatted_value = value
    else:
        formatted_value = value.__repr__()

    return "{key}={value}".format(key=key, value=formatted_value)


def parse_schema_test(test_base, model_name, test_config, test_type,
                      root_project_config, package_project_config,
                      all_projects):

    if isinstance(test_config, (basestring, int, float, bool)):
        test_args = {'arg': test_config}
    else:
        test_args = test_config

    # sort the dict so the keys are rendered deterministically (for tests)
    kwargs = [as_kwarg(key, test_args[key]) for key in sorted(test_args)]

    raw_sql = "{{{{ {macro}(model=ref('{model}'), {kwargs}) }}}}".format(**{
        'model': model_name,
        'macro': "test_{}".format(test_type),
        'kwargs': ", ".join(kwargs)
    })

    name = get_nice_schema_test_name(test_type, model_name, test_args)

    pseudo_path = dbt.utils.get_pseudo_test_path(name, test_base.get('path'),
                                                 'schema_test')
    to_return = {
        'name': name,
        'resource_type': test_base.get('resource_type'),
        'package_name': test_base.get('package_name'),
        'root_path': test_base.get('root_path'),
        'path': pseudo_path,
        'raw_sql': raw_sql
    }

    return parse_node(to_return,
                      get_test_path(test_base.get('package_name'),
                                    name),
                      root_project_config,
                      package_project_config,
                      all_projects,
                      tags={'schema'},
                      fqn_extra=None)


def load_and_parse_yml(package_name, root_project, all_projects, root_dir,
                       relative_dirs):
    extension = "[!.#~]*.yml"

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = []

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'), strip=False)

        parts = dbt.utils.split_path(file_match.get('relative_path', ''))
        name, _ = os.path.splitext(parts[-1])

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': NodeType.Test,
            'path': file_match.get('relative_path'),
            'package_name': package_name,
            'raw_yml': file_contents
        })

    return parse_schema_tests(result, root_project, all_projects)


def parse_archives_from_projects(root_project, all_projects):
    archives = []
    to_return = {}

    for name, project in all_projects.items():
        archives = archives + parse_archives_from_project(project)

    for archive in archives:
        node_path = get_path(archive.get('resource_type'),
                             archive.get('package_name'),
                             archive.get('name'))

        to_return[node_path] = parse_node(
            archive,
            node_path,
            root_project,
            all_projects.get(archive.get('package_name')),
            all_projects)

    return to_return


def parse_archives_from_project(project):
    archives = []
    archive_configs = project.get('archive', [])

    for archive_config in archive_configs:
        tables = archive_config.get('tables')

        if tables is None:
            continue

        for table in tables:
            config = table.copy()
            config['source_schema'] = archive_config.get('source_schema')
            config['target_schema'] = archive_config.get('target_schema')

            fake_path = [config['target_schema'], config['target_table']]
            archives.append({
                'name': table.get('target_table'),
                'root_path': project.get('project-root'),
                'resource_type': NodeType.Archive,
                'path': os.path.join('archive', *fake_path),
                'package_name': project.get('name'),
                'config': config,
                'raw_sql': '-- noop'
            })

    return archives
