import copy
import os
import re
import hashlib
import collections

import dbt.exceptions
import dbt.flags
import dbt.model
import dbt.utils
import dbt.hooks

import jinja2.runtime
import dbt.clients.jinja
import dbt.clients.yaml_helper
import dbt.clients.agate_helper

import dbt.context.parser

import dbt.contracts.graph.parsed
import dbt.contracts.graph.unparsed
import dbt.contracts.project

from dbt.node_types import NodeType, RunHookType
from dbt.compat import basestring, to_string
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_pseudo_test_path, coalesce


def get_path(resource_type, package_name, resource_name):
    return "{}.{}.{}".format(resource_type, package_name, resource_name)


def get_test_path(package_name, resource_name):
    return get_path(NodeType.Test, package_name, resource_name)


def resolve_ref(flat_graph, target_model_name, target_model_package,
                current_project, node_package):

    if target_model_package is not None:
        return dbt.utils.find_refable_by_name(
            flat_graph,
            target_model_name,
            target_model_package)

    target_model = None

    # first pass: look for models in the current_project
    target_model = dbt.utils.find_refable_by_name(
        flat_graph,
        target_model_name,
        current_project)

    if target_model is not None and dbt.utils.is_enabled(target_model):
        return target_model

    # second pass: look for models in the node's package
    target_model = dbt.utils.find_refable_by_name(
        flat_graph,
        target_model_name,
        node_package)

    if target_model is not None and dbt.utils.is_enabled(target_model):
        return target_model

    # final pass: look for models in any package
    # todo: exclude the packages we have already searched. overriding
    # a package model in another package doesn't necessarily work atm
    return dbt.utils.find_refable_by_name(
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
                # This may raise. Even if it doesn't, we don't want to add
                # this node to the graph b/c there is no destination node
                node.get('config', {})['enabled'] = False
                dbt.utils.invalid_ref_fail_unless_test(node,
                                                       target_model_name,
                                                       target_model_package)
                continue

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
        'path': macro_file_path,
        'original_file_path': macro_file_path,
        'resource_type': NodeType.Macro,
        'package_name': package_name,
        'depends_on': {
            'macros': [],
        }
    }

    try:
        template = dbt.clients.jinja.get_template(
            macro_file_contents, context, node=base_node)
    except dbt.exceptions.CompilationException as e:
        e.node = base_node
        raise e

    for key, item in template.module.__dict__.items():
        if type(item) == jinja2.runtime.Macro:
            name = key.replace(dbt.utils.MACRO_PREFIX, '')

            unique_id = get_path(NodeType.Macro,
                                 package_name,
                                 name)

            new_node = base_node.copy()
            new_node.update({
                'name': name,
                'unique_id': unique_id,
                'tags': tags,
                'root_path': root_path,
                'path': macro_file_path,
                'original_file_path': macro_file_path,
                'raw_sql': macro_file_contents,
            })

            new_node['generator'] = dbt.clients.jinja.macro_generator(
                template, new_node)

            to_return[unique_id] = new_node

    return to_return


def parse_node(node, node_path, root_project_config, package_project_config,
               all_projects, tags=None, fqn_extra=None, fqn=None, macros=None):
    logger.debug("Parsing {}".format(node_path))
    node = copy.deepcopy(node)

    tags = coalesce(tags, set())
    fqn_extra = coalesce(fqn_extra, [])
    macros = coalesce(macros, {})

    node.update({
        'refs': [],
        'depends_on': {
            'nodes': [],
            'macros': [],
        }
    })

    if fqn is None:
        fqn = get_fqn(node.get('path'), package_project_config, fqn_extra)

    config = dbt.model.SourceConfig(
        root_project_config,
        package_project_config,
        fqn,
        node['resource_type'])

    node['unique_id'] = node_path
    node['empty'] = ('raw_sql' in node and len(node['raw_sql'].strip()) == 0)
    node['fqn'] = fqn
    node['tags'] = tags
    node['config_reference'] = config

    # Set this temporarily. Not the full config yet (as config() hasn't been
    # called from jinja yet). But the Var() call below needs info about project
    # level configs b/c they might contain refs. TODO: Restructure this?
    config_dict = node.get('config', {})
    config_dict.update(config.config)
    node['config'] = config_dict

    # Set this temporarily so get_rendered() below has access to a schema
    profile = dbt.utils.get_profile_from_project(root_project_config)
    default_schema = profile.get('schema', 'public')
    node['schema'] = default_schema

    context = dbt.context.parser.generate(node, root_project_config,
                                          {"macros": macros})

    dbt.clients.jinja.get_rendered(
        node.get('raw_sql'), context, node,
        capture_macros=True)

    # Clean up any open connections opened by adapter functions that hit the db
    db_wrapper = context['adapter']
    adapter = db_wrapper.adapter
    profile = db_wrapper.profile
    adapter.release_connection(profile, node.get('name'))

    # Special macro defined in the global project
    schema_override = config.config.get('schema')
    get_schema = context.get('generate_schema_name', lambda x: default_schema)
    node['schema'] = get_schema(schema_override)

    # Overwrite node config
    config_dict = node.get('config', {})
    config_dict.update(config.config)
    node['config'] = config_dict

    for hook_type in dbt.hooks.ModelHookType.Both:
        node['config'][hook_type] = dbt.hooks.get_hooks(node, hook_type)

    del node['config_reference']

    return node


def parse_sql_nodes(nodes, root_project, projects, tags=None, macros=None):
    if tags is None:
        tags = set()

    if macros is None:
        macros = {}

    to_return = {}

    dbt.contracts.graph.unparsed.validate_nodes(nodes)

    for node in nodes:
        package_name = node.get('package_name')

        node_path = get_path(node.get('resource_type'),
                             package_name,
                             node.get('name'))

        node_parsed = parse_node(node,
                                 node_path,
                                 root_project,
                                 projects.get(package_name),
                                 projects,
                                 tags=tags,
                                 macros=macros)

        # Ignore disabled nodes
        if not node_parsed['config']['enabled']:
            continue

        # Check for duplicate model names
        existing_node = to_return.get(node_path)
        if existing_node is not None:
            dbt.exceptions.raise_duplicate_resource_name(
                    existing_node, node_parsed)

        to_return[node_path] = node_parsed

    dbt.contracts.graph.parsed.validate_nodes(to_return)

    return to_return


def load_and_parse_sql(package_name, root_project, all_projects, root_dir,
                       relative_dirs, resource_type, tags=None, macros=None):
    extension = "[!.#~]*.sql"

    if tags is None:
        tags = set()

    if macros is None:
        macros = {}

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

        original_file_path = os.path.join(
            file_match.get('searched_path'),
            path)

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': resource_type,
            'path': path,
            'original_file_path': original_file_path,
            'package_name': package_name,
            'raw_sql': file_contents
        })

    return parse_sql_nodes(result, root_project, all_projects, tags, macros)


def get_hooks_from_project(project_cfg, hook_type):
    hooks = project_cfg.get(hook_type, [])

    if type(hooks) not in (list, tuple):
        hooks = [hooks]

    return hooks


def get_hooks(all_projects, hook_type):
    project_hooks = collections.defaultdict(list)

    for project_name, project in all_projects.items():
        hooks = get_hooks_from_project(project, hook_type)
        project_hooks[project_name].extend(hooks)

    return project_hooks


def load_and_parse_run_hook_type(root_project, all_projects, hook_type,
                                 macros=None):

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    project_hooks = get_hooks(all_projects, hook_type)

    result = []
    for project_name, hooks in project_hooks.items():
        project = all_projects[project_name]

        for i, hook in enumerate(hooks):
            hook_name = '{}-{}-{}'.format(project_name, hook_type, i)
            hook_path = dbt.utils.get_pseudo_hook_path(hook_name)

            result.append({
                'name': hook_name,
                'root_path': "{}/dbt_project.yml".format(project_name),
                'resource_type': NodeType.Operation,
                'path': hook_path,
                'original_file_path': hook_path,
                'package_name': project_name,
                'raw_sql': hook,
                'index': i
            })

    tags = {hook_type}
    return parse_sql_nodes(result, root_project, all_projects, tags=tags,
                           macros=macros)


def load_and_parse_run_hooks(root_project, all_projects, macros=None):
    if macros is None:
        macros = {}

    hook_nodes = {}
    for hook_type in RunHookType.Both:
        project_hooks = load_and_parse_run_hook_type(root_project,
                                                     all_projects,
                                                     hook_type,
                                                     macros=macros)
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


def get_parsed_schema_test(test_node, test_type, model_name, config,
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
        desc = '"{}" test on model "{}"'.format(original_test_type, model_name)
        dbt.exceptions.raise_dep_not_found(test_node, desc, test_namespace)

    return parse_schema_test(
        test_node,
        model_name,
        config,
        test_namespace,
        test_type,
        root_project,
        source_package,
        all_projects=projects,
        macros=macros)


def parse_schema_tests(tests, root_project, projects, macros=None):
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

        no_tests_warning = ("* WARNING: No constraints found for model"
                            " '{}' in file {}\n")
        for model_name, test_spec in test_yml.items():
            if test_spec is None or test_spec.get('constraints') is None:
                test_path = test.get('original_file_path', '<unknown>')
                logger.warning(no_tests_warning.format(model_name, test_path))
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
                    to_add = get_parsed_schema_test(
                                test, test_type, model_name, config,
                                root_project, projects, macros)

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


def parse_schema_test(test_base, model_name, test_config, test_namespace,
                      test_type, root_project_config, package_project_config,
                      all_projects, macros=None):

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

    raw_sql = "{{{{ {macro}(model=ref('{model}'), {kwargs}) }}}}".format(**{
        'model': model_name,
        'macro': macro_name,
        'kwargs': ", ".join(kwargs)
    })

    base_path = test_base.get('path')
    hashed_name, full_name = get_nice_schema_test_name(test_type, model_name,
                                                       test_args)

    hashed_path = get_pseudo_test_path(hashed_name, base_path, 'schema_test')
    full_path = get_pseudo_test_path(full_name, base_path, 'schema_test')

    # supply our own fqn which overrides the hashed version from the path
    fqn_override = get_fqn(full_path, package_project_config)

    to_return = {
        'name': full_name,
        'resource_type': test_base.get('resource_type'),
        'package_name': test_base.get('package_name'),
        'root_path': test_base.get('root_path'),
        'path': hashed_path,
        'original_file_path': test_base.get('original_file_path'),
        'raw_sql': raw_sql
    }

    return parse_node(to_return,
                      get_test_path(test_base.get('package_name'),
                                    full_name),
                      root_project_config,
                      package_project_config,
                      all_projects,
                      tags={'schema'},
                      fqn_extra=None,
                      fqn=fqn_override,
                      macros=macros)


def load_and_parse_yml(package_name, root_project, all_projects, root_dir,
                       relative_dirs, macros=None):
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

    return parse_schema_tests(result, root_project, all_projects, macros)


def parse_archives_from_projects(root_project, all_projects, macros=None):
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
            all_projects,
            macros=macros)

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
                'original_file_path': 'dbt_project.yml',
                'package_name': project.get('name'),
                'config': config,
                'raw_sql': '{{config(materialized="archive")}} -- noop'
            })

    return archives


def parse_seed_file(file_match, root_dir, package_name):
    abspath = file_match['absolute_path']
    logger.debug("Parsing {}".format(abspath))
    to_return = {}
    table_name = os.path.basename(abspath)[:-4]
    node = {
        'unique_id': get_path(NodeType.Seed, package_name, table_name),
        'path': file_match['relative_path'],
        'name': table_name,
        'root_path': root_dir,
        'resource_type': NodeType.Seed,
        # Give this raw_sql so it conforms to the node spec,
        # use dummy text so it doesn't look like an empty node
        'raw_sql': '-- csv --',
        'package_name': package_name,
        'depends_on': {'nodes': []},
        'original_file_path': os.path.join(file_match.get('searched_path'),
                                           file_match.get('relative_path')),
    }
    try:
        table = dbt.clients.agate_helper.from_csv(abspath)
    except ValueError as e:
        dbt.exceptions.raise_compiler_error(str(e), node)
    table.original_abspath = abspath
    node['agate_table'] = table
    return node


def load_and_parse_seeds(package_name, root_project, all_projects, root_dir,
                         relative_dirs, resource_type, tags=None, macros=None):
    extension = "[!.#~]*.csv"
    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)
    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)
    result = {}
    for file_match in file_matches:
        node = parse_seed_file(file_match, root_dir, package_name)
        node_path = node['unique_id']
        parsed = parse_node(node, node_path, root_project,
                            all_projects.get(package_name),
                            all_projects, tags=tags, macros=macros)
        # parsed['empty'] = False
        result[node_path] = parsed

    dbt.contracts.graph.parsed.validate_nodes(result)
    return result
