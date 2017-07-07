import os
import json
import hashlib
import itertools

from dbt.include import GLOBAL_DBT_MODULES_PATH
from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType

import dbt.clients.jinja


DBTConfigKeys = [
    'enabled',
    'materialized',
    'dist',
    'sort',
    'sql_where',
    'unique_key',
    'sort_type',
    'pre-hook',
    'post-hook',
    'vars'
]


class ExitCodes(object):
    Success = 0
    ModelError = 1
    UnhandledError = 2


class This(object):
    def __init__(self, schema, table, name):
        self.schema = schema
        self.table = table
        self.name = table if name is None else name

    def schema_table(self, schema, table):
        return '"{}"."{}"'.format(schema, table)

    def __repr__(self):
        return self.schema_table(self.schema, self.table)


def get_model_name_or_none(model):
    if model is None:
        name = '<None>'

    elif isinstance(model, basestring):
        name = model
    elif isinstance(model, dict):
        name = model.get('name')
    else:
        name = model.nice_name
    return name


def compiler_error(model, msg):
    name = get_model_name_or_none(model)
    raise RuntimeError(
        "! Compilation error while compiling model {}:\n! {}\n"
        .format(name, msg)
    )


def compiler_warning(model, msg):
    name = get_model_name_or_none(model)
    logger.info(
        "* Compilation warning while compiling model {}:\n* {}\n"
        .format(name, msg)
    )


class Var(object):
    UndefinedVarError = "Required var '{}' not found in config:\nVars "\
                        "supplied to {} = {}"
    NoneVarError = "Supplied var '{}' is undefined in config:\nVars supplied "\
                   "to {} = {}"

    def __init__(self, model, context):
        self.model = model
        self.context = context

        if isinstance(model, dict) and model.get('unique_id'):
            self.local_vars = model.get('config', {}).get('vars')
            self.model_name = model.get('name')
        else:
            # still used for wrapping
            self.model_name = model.nice_name
            self.local_vars = model.config.get('vars', {})

    def pretty_dict(self, data):
        return json.dumps(data, sort_keys=True, indent=4)

    def __call__(self, var_name, default=None):
        pretty_vars = self.pretty_dict(self.local_vars)
        if var_name not in self.local_vars and default is None:
            compiler_error(
                self.model,
                self.UndefinedVarError.format(
                    var_name, self.model_name, pretty_vars
                )
            )
        elif var_name in self.local_vars:
            raw = self.local_vars[var_name]
            if raw is None:
                model_name = get_model_name_or_none(self.model)
                compiler_error(
                    self.model,
                    self.NoneVarError.format(
                        var_name, model_name, pretty_vars
                    )
                )

            # if bool/int/float/etc are passed in, don't compile anything
            if not isinstance(raw, basestring):
                return raw

            return dbt.clients.jinja.get_rendered(raw, self.context)
        else:
            return default


def model_cte_name(model):
    return '__dbt__CTE__{}'.format(model.get('name'))


def model_immediate_name(model, non_destructive):
    "The name of the model table/view within the transaction"
    model_name = model.get('name')
    if non_destructive or get_materialization(model) == 'incremental':
        return model_name
    else:
        return "{}__dbt_tmp".format(model_name)


def find_model_by_name(flat_graph, target_name, target_package):
    return find_by_name(flat_graph, target_name, target_package,
                        'nodes', NodeType.Model)


def find_macro_by_name(flat_graph, target_name, target_package):
    return find_by_name(flat_graph, target_name, target_package,
                        'macros', NodeType.Macro)


def find_by_name(flat_graph, target_name, target_package, subgraph,
                 nodetype):
    for name, model in flat_graph.get(subgraph).items():
        node_parts = name.split('.')
        if len(node_parts) != 3:
            node_type = model.get('resource_type', 'node')
            msg = "{} names cannot contain '.' characters".format(node_type)
            dbt.exceptions.raise_compiler_error(model, msg)

        resource_type, package_name, node_name = node_parts

        if (resource_type == nodetype and
            ((target_name == node_name) and
             (target_package is None or
              target_package == package_name))):
            return model

    return None


def find_model_by_fqn(models, fqn):
    for model in models:
        if tuple(model.fqn) == tuple(fqn):
            return model

    raise RuntimeError(
        "Couldn't find a compiled model with fqn: '{}'".format(fqn)
    )


def dependency_projects(project):
    import dbt.project
    module_paths = [
        GLOBAL_DBT_MODULES_PATH,
        os.path.join(project['project-root'], project['modules-path'])
    ]

    for module_path in module_paths:
        logger.debug("Loading dependency project from {}".format(module_path))

        for obj in os.listdir(module_path):
            full_obj = os.path.join(module_path, obj)

            if not os.path.isdir(full_obj) or obj.startswith('__'):
                # exclude non-dirs and dirs that start with __
                # the latter could be something like __pycache__
                # for the global dbt modules dir
                continue

            try:
                yield dbt.project.read_project(
                    os.path.join(full_obj, 'dbt_project.yml'),
                    project.profiles_dir,
                    profile_to_load=project.profile_to_load,
                    args=project.args)
            except dbt.project.DbtProjectError as e:
                logger.info(
                    "Error reading dependency project at {}".format(
                        full_obj)
                )
                logger.info(str(e))


def split_path(path):
    return path.split(os.sep)


# http://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data
def deep_merge(destination, source):
    if isinstance(source, dict):
        for key, value in source.items():
            if isinstance(value, dict):
                node = destination.setdefault(key, {})
                deep_merge(node, value)
            elif isinstance(value, tuple) or isinstance(value, list):
                if key in destination:
                    destination[key] = list(value) + list(destination[key])
                else:
                    destination[key] = value
            else:
                destination[key] = value
        return destination


def to_unicode(s, encoding):
    try:
        unicode
        return unicode(s, encoding)
    except NameError:
        return s


def to_string(s):
    try:
        unicode
        return s.encode('utf-8')
    except NameError:
        return s


def is_blocking_dependency(node):
    return (is_type(node, NodeType.Model))


def get_materialization(node):
    return node.get('config', {}).get('materialized')


def is_enabled(node):
    return node.get('config', {}).get('enabled') is True


def is_type(node, _type):
    return node.get('resource_type') == _type


def get_pseudo_test_path(node_name, source_path, test_type):
    "schema tests all come from schema.yml files. fake a source sql file"
    source_path_parts = split_path(source_path)
    source_path_parts.pop()  # ignore filename
    suffix = [test_type, "{}.sql".format(node_name)]
    pseudo_path_parts = source_path_parts + suffix
    return os.path.join(*pseudo_path_parts)


def get_pseudo_hook_path(hook_name):
    path_parts = ['hooks', "{}.sql".format(hook_name)]
    return os.path.join(*path_parts)


def get_nodes_by_tags(nodes, match_tags, resource_type):
    matched_nodes = []
    for node in nodes:
        node_tags = node.get('tags', set())
        if len(node_tags & match_tags):
            matched_nodes.append(node)
    return matched_nodes


def get_hash(model):
    return hashlib.md5(model.get('unique_id').encode('utf-8')).hexdigest()


def get_hashed_contents(model):
    return hashlib.md5(model.get('raw_sql').encode('utf-8')).hexdigest()


def flatten_nodes(dep_list):
    return list(itertools.chain.from_iterable(dep_list))
