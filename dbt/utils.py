import os
import hashlib
import itertools
import collections
import copy
import functools

import dbt.exceptions
import dbt.flags

from dbt.include import GLOBAL_DBT_MODULES_PATH
from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.clients import yaml_helper


DBTConfigKeys = [
    'alias',
    'schema',
    'enabled',
    'materialized',
    'dist',
    'sort',
    'sql_where',
    'unique_key',
    'sort_type',
    'pre-hook',
    'post-hook',
    'vars',
    'column_types',
    'bind',
    'quoting',
]


class ExitCodes(object):
    Success = 0
    ModelError = 1
    UnhandledError = 2


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_profile_from_project(project):
    target_name = project.get('target', {})
    profile = project.get('outputs', {}).get(target_name, {})
    return profile


def get_model_name_or_none(model):
    if model is None:
        name = '<None>'

    elif isinstance(model, basestring):
        name = model
    elif isinstance(model, dict):
        name = model['alias']
    else:
        name = model.nice_name
    return name


def compiler_warning(model, msg):
    name = get_model_name_or_none(model)
    logger.info(
        "* Compilation warning while compiling model {}:\n* {}\n"
        .format(name, msg)
    )


def model_immediate_name(model, non_destructive):
    """
    Returns the name of the model relation within the transaction. This is
    useful for referencing the model in pre or post hooks. Non-destructive
    models aren't created with temp suffixes, nor are incremental models or
    seeds.
    """

    model_name = model['alias']
    is_incremental = (get_materialization(model) == 'incremental')
    is_seed = is_type(model, 'seed')

    if non_destructive or is_incremental or is_seed:
        return model_name
    else:
        return "{}__dbt_tmp".format(model_name)


def find_refable_by_name(flat_graph, target_name, target_package):
    return find_by_name(flat_graph, target_name, target_package,
                        'nodes', NodeType.refable())


def find_macro_by_name(flat_graph, target_name, target_package):
    return find_by_name(flat_graph, target_name, target_package,
                        'macros', [NodeType.Macro])


def find_operation_by_name(flat_graph, target_name, target_package):
    return find_by_name(flat_graph, target_name, target_package,
                        'macros', [NodeType.Operation])


def find_by_name(flat_graph, target_name, target_package, subgraph,
                 nodetype):
    return find_in_subgraph_by_name(
        flat_graph.get(subgraph),
        target_name,
        target_package,
        nodetype)


def find_in_subgraph_by_name(subgraph, target_name, target_package, nodetype):
    """Find an entry in a subgraph by name. Any mapping that implements
    .items() and maps unique id -> something can be used as the subgraph.

    Names are like:
        '{nodetype}.{target_package}.{target_name}'

    You can use `None` for the package name as a wildcard.
    """
    for name, model in subgraph.items():
        node_parts = name.split('.')
        if len(node_parts) != 3:
            node_type = model.get('resource_type', 'node')
            msg = "{} names cannot contain '.' characters".format(node_type)
            dbt.exceptions.raise_compiler_error(msg, model)

        resource_type, package_name, node_name = node_parts

        if (resource_type in nodetype and
            ((target_name == node_name) and
             (target_package is None or
              target_package == package_name))):
            return model

    return None


MACRO_PREFIX = 'dbt_macro__'
OPERATION_PREFIX = 'dbt_operation__'


def get_dbt_macro_name(name):
    return '{}{}'.format(MACRO_PREFIX, name)


def get_dbt_operation_name(name):
    return '{}{}'.format(OPERATION_PREFIX, name)


def get_materialization_macro_name(materialization_name, adapter_type=None,
                                   with_prefix=True):
    if adapter_type is None:
        adapter_type = 'default'

    name = 'materialization_{}_{}'.format(materialization_name, adapter_type)

    if with_prefix:
        return get_dbt_macro_name(name)
    else:
        return name


def get_materialization_macro(flat_graph, materialization_name,
                              adapter_type=None):
    macro_name = get_materialization_macro_name(materialization_name,
                                                adapter_type,
                                                with_prefix=False)

    macro = find_macro_by_name(
        flat_graph,
        macro_name,
        None)

    if adapter_type not in ('default', None) and macro is None:
        macro_name = get_materialization_macro_name(materialization_name,
                                                    adapter_type='default',
                                                    with_prefix=False)
        macro = find_macro_by_name(
            flat_graph,
            macro_name,
            None)

    return macro


def get_operation_macro_name(operation_name, with_prefix=True):
    if with_prefix:
        return get_dbt_operation_name(operation_name)
    else:
        return operation_name


def get_operation_macro(flat_graph, operation_name):
    name = get_operation_macro_name(operation_name, with_prefix=False)
    return find_operation_by_name(flat_graph, name, None)


def load_project_with_profile(source_project, project_dir):
    project_filepath = os.path.join(project_dir, 'dbt_project.yml')
    return dbt.project.read_project(
        project_filepath,
        source_project.profiles_dir,
        profile_to_load=source_project.profile_to_load,
        args=source_project.args)


def dependencies_for_path(project, module_path):
    """Given a module path, yield all dependencies in that path."""
    import dbt.project
    logger.debug("Loading dependency project from {}".format(module_path))

    for obj in os.listdir(module_path):
        full_obj = os.path.join(module_path, obj)

        if not os.path.isdir(full_obj) or obj.startswith('__'):
            # exclude non-dirs and dirs that start with __
            # the latter could be something like __pycache__
            # for the global dbt modules dir
            continue

        try:
            yield load_project_with_profile(project, full_obj)
        except dbt.project.DbtProjectError as e:
            logger.info(
                "Error reading dependency project at {}".format(
                    full_obj)
            )
            logger.info(str(e))


def dependency_projects(project):
    module_paths = [
        GLOBAL_DBT_MODULES_PATH,
        os.path.join(project['project-root'], project['modules-path'])
    ]

    for module_path in module_paths:
        for entry in dependencies_for_path(project, module_path):
            yield entry


def split_path(path):
    return path.split(os.sep)


def merge(*args):
    if len(args) == 0:
        return None

    if len(args) == 1:
        return args[0]

    lst = list(args)
    last = lst.pop(len(lst)-1)

    return _merge(merge(*lst), last)


def _merge(a, b):
    to_return = a.copy()
    to_return.update(b)
    return to_return


# http://stackoverflow.com/questions/20656135/python-deep-merge-dictionary-data
def deep_merge(*args):
    """
    >>> dbt.utils.deep_merge({'a': 1, 'b': 2, 'c': 3}, {'a': 2}, {'a': 3, 'b': 1})  # noqa
    {'a': 3, 'b': 1, 'c': 3}
    """
    if len(args) == 0:
        return None

    if len(args) == 1:
        return copy.deepcopy(args[0])

    lst = list(args)
    last = copy.deepcopy(lst.pop(len(lst)-1))

    return _deep_merge(deep_merge(*lst), last)


def _deep_merge(destination, source):
    if isinstance(source, dict):
        for key, value in source.items():
            deep_merge_item(destination, key, value)
        return destination


def deep_merge_item(destination, key, value):
    if isinstance(value, dict):
        node = destination.setdefault(key, {})
        destination[key] = deep_merge(node, value)
    elif isinstance(value, tuple) or isinstance(value, list):
        if key in destination:
            destination[key] = list(value) + list(destination[key])
        else:
            destination[key] = value
    else:
        destination[key] = value


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


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
        node_tags = node.get('tags', [])
        if len(set(node_tags) & match_tags):
            matched_nodes.append(node)
    return matched_nodes


def md5(string):
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def get_hash(model):
    return hashlib.md5(model.get('unique_id').encode('utf-8')).hexdigest()


def get_hashed_contents(model):
    return hashlib.md5(model.get('raw_sql').encode('utf-8')).hexdigest()


def flatten_nodes(dep_list):
    return list(itertools.chain.from_iterable(dep_list))


class memoized(object):
    '''Decorator. Caches a function's return value each time it is called. If
    called later with the same arguments, the cached value is returned (not
    reevaluated).

    Taken from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize'''
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        value = self.func(*args)
        self.cache[args] = value
        return value

    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__

    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)


def invalid_ref_fail_unless_test(node, target_model_name,
                                 target_model_package):
    if node.get('resource_type') == NodeType.Test:
        warning = dbt.exceptions.get_target_not_found_msg(
                    node,
                    target_model_name,
                    target_model_package)
        logger.debug("WARNING: {}".format(warning))
    else:
        dbt.exceptions.ref_target_not_found(
            node,
            target_model_name,
            target_model_package)


def parse_cli_vars(var_string):
    try:
        cli_vars = yaml_helper.load_yaml_text(var_string)
        var_type = type(cli_vars)
        if var_type == dict:
            return cli_vars
        else:
            type_name = var_type.__name__
            dbt.exceptions.raise_compiler_error(
                "The --vars argument must be a YAML dictionary, but was "
                "of type '{}'".format(type_name))
    except dbt.exceptions.ValidationException as e:
        logger.error(
                "The YAML provided in the --vars argument is not valid.\n")
        raise


def filter_null_values(input):
    return dict((k, v) for (k, v) in input.items()
                if v is not None)


def add_ephemeral_model_prefix(s):
    return '__dbt__CTE__{}'.format(s)
