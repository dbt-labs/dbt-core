import copy
import os
import yaml

import dbt.flags
import dbt.model
import dbt.utils

import dbt.clients.jinja

import dbt.contracts.graph.parsed
import dbt.contracts.graph.unparsed
import dbt.contracts.project

from dbt.model import NodeType
from dbt.logger import GLOBAL_LOGGER as logger

QUERY_VALIDATE_NOT_NULL = """
with validation as (
  select {field} as f
  from {ref}
)
select count(*) from validation where f is null
"""


QUERY_VALIDATE_UNIQUE = """
with validation as (
  select {field} as f
  from {ref}
  where {field} is not null
),
validation_errors as (
    select f from validation group by f having count(*) > 1
)
select count(*) from validation_errors
"""


QUERY_VALIDATE_ACCEPTED_VALUES = """
with all_values as (
  select distinct {field} as f
  from {ref}
),
validation_errors as (
    select f from all_values where f not in ({values_csv})
)
select count(*) from validation_errors
"""


QUERY_VALIDATE_REFERENTIAL_INTEGRITY = """
with parent as (
  select {parent_field} as id
  from {parent_ref}
), child as (
  select {child_field} as id
  from {child_ref}
)
select count(*) from child
where id not in (select id from parent) and id is not null
"""


def get_path(resource_type, package_name, resource_name):
    return "{}.{}.{}".format(resource_type, package_name, resource_name)


def get_model_path(package_name, resource_name):
    return get_path(NodeType.Model, package_name, resource_name)


def get_test_path(package_name, resource_name):
    return get_path(NodeType.Test, package_name, resource_name)


def get_macro_path(package_name, resource_name):
    return get_path('macros', package_name, resource_name)


def __ref(model):

    def ref(*args):
        pass

    return ref


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


def get_fqn(path, package_project_config, extra=[]):
    parts = dbt.utils.split_path(path)
    name, _ = os.path.splitext(parts[-1])
    fqn = ([package_project_config.get('name')] +
           parts[:-1] +
           extra +
           [name])

    return fqn


def parse_node(node, node_path, root_project_config, package_project_config,
               macro_generator=None, tags=[], fqn_extra=[]):
    logger.debug("Parsing {}".format(node_path))
    parsed_node = copy.deepcopy(node)

    parsed_node.update({
        'depends_on': [],
    })

    fqn = get_fqn(node.get('path'), package_project_config, fqn_extra)

    config = dbt.model.SourceConfig(
        root_project_config, package_project_config, fqn)

    context = {}

    context['ref'] = __ref(parsed_node)
    context['config'] = __config(parsed_node, config)
    context['var'] = lambda *args: ''
    context['target'] = property(lambda x: '', lambda x: x)
    context['this'] = ''

    if macro_generator is not None:
        for macro_data in macro_generator(context):
            macro = macro_data["macro"]
            macro_name = macro_data["name"]
            project = macro_data["project"]

            if context.get(project.get('name')) is None:
                context[project.get('name')] = {}

            context.get(project.get('name'), {}) \
                   .update({macro_name: macro})

            if node.get('package_name') == project.get('name'):
                context.update({macro_name: macro})

    dbt.clients.jinja.get_rendered(
        node.get('raw_sql'), context, node, silent_on_undefined=True)

    config_dict = node.get('config', {})
    config_dict.update(config.config)

    parsed_node['unique_id'] = node_path
    parsed_node['config'] = config_dict
    parsed_node['empty'] = (len(node.get('raw_sql').strip()) == 0)
    parsed_node['fqn'] = fqn
    parsed_node['tags'] = tags

    return parsed_node


def parse_sql_nodes(nodes, root_project, projects, macro_generator=None,
                    tags=[]):
    to_return = {}

    dbt.contracts.graph.unparsed.validate(nodes)

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
                                          macro_generator,
                                          tags=tags)

    dbt.contracts.graph.parsed.validate(to_return)

    return to_return


def load_and_parse_sql(package_name, root_project, all_projects, root_dir,
                       relative_dirs, resource_type, macro_generator, tags=[]):
    extension = "[!.#~]*.sql"

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

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': resource_type,
            'path': file_match.get('relative_path'),
            'package_name': package_name,
            'raw_sql': file_contents
        })

    return parse_sql_nodes(result, root_project, all_projects, macro_generator,
                           tags)


def parse_schema_tests(tests, root_project, projects):
    to_return = {}

    for test in tests:
        test_yml = yaml.safe_load(test.get('raw_yml'))

        # validate schema test yml structure

        for model_name, test_spec in test_yml.items():
            for test_type, configs in test_spec.get('constraints', {}).items():
                for config in configs:
                    to_add = parse_schema_test(
                        test, model_name, config, test_type,
                        root_project,
                        projects.get(test.get('package_name')))

                    if to_add is not None:
                        to_return[to_add.get('unique_id')] = to_add

    return to_return


def parse_schema_test(test_base, model_name, test_config, test_type,
                      root_project_config, package_project_config):
    if test_type == 'not_null':
        raw_sql = QUERY_VALIDATE_NOT_NULL.format(
            ref="{{ref('"+model_name+"')}}", field=test_config)
        name_key = test_config

    elif test_type == 'unique':
        raw_sql = QUERY_VALIDATE_UNIQUE.format(
            ref="{{ref('"+model_name+"')}}", field=test_config)
        name_key = test_config

    elif test_type == 'relationships':
        if not isinstance(test_config, dict):
            return None

        child_field = test_config.get('from')
        parent_field = test_config.get('field')
        parent_model = test_config.get('to')

        raw_sql = QUERY_VALIDATE_REFERENTIAL_INTEGRITY.format(
            child_field=child_field,
            child_ref="{{ref('"+model_name+"')}}",
            parent_field=parent_field,
            parent_ref=("{{ref('"+parent_model+"')}}"))

        name_key = '{}_to_{}_{}'.format(child_field, parent_model,
                                        parent_field)

    elif test_type == 'accepted_values':
        if not isinstance(test_config, dict):
            return None

        raw_sql = QUERY_VALIDATE_ACCEPTED_VALUES.format(
            ref="{{ref('"+model_name+"')}}",
            field=test_config.get('field', ''),
            values_csv="'{}'".format(
                "','".join([str(v) for v in test_config.get('values', [])])))

        name_key = test_config.get('field')

    else:
        raise dbt.exceptions.ValidationException(
            'Unknown schema test type {}'.format(test_type))

    name = '{}_{}_{}'.format(test_type, model_name, name_key)

    to_return = {
        'name': name,
        'resource_type': test_base.get('resource_type'),
        'package_name': test_base.get('package_name'),
        'root_path': test_base.get('root_path'),
        'path': test_base.get('path'),
        'raw_sql': raw_sql
    }

    return parse_node(to_return,
                      get_test_path(test_base.get('package_name'),
                                    name),
                      root_project_config,
                      package_project_config,
                      tags=['schema'],
                      fqn_extra=['schema'])


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
            file_match.get('absolute_path'))

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
            all_projects.get(archive.get('package_name')))

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

            archives.append({
                'name': table.get('target_table'),
                'root_path': project.get('project-root'),
                'resource_type': NodeType.Archive,
                'path': project.get('project-root'),
                'package_name': project.get('name'),
                'config': config,
                'raw_sql': '-- noop'
            })

    return archives
