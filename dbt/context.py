from dbt.utils import get_materialization, compiler_error
from dbt.adapters.factory import get_adapter
from dbt.compat import basestring

import dbt.clients.jinja
import dbt.flags

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


def get_sort_qualifier(model, project):
    model_config = model.get('config', {})

    if 'sort' not in model['config']:
        return ''

    if get_materialization(model) not in ('table', 'incremental'):
        return ''

    sort_keys = model_config.get('sort')
    sort_type = model_config.get('sort_type', 'compound')

    if not isinstance(sort_type, basestring):
        compiler_error(
            model,
            "The provided sort_type '{}' is not valid!".format(sort_type)
        )

    sort_type = sort_type.strip().lower()

    adapter = get_adapter(project.run_environment())
    return adapter.sort_qualifier(sort_type, sort_keys)


def get_dist_qualifier(model, project):
    model_config = model.get('config', {})

    if 'dist' not in model_config:
        return ''

    if get_materialization(model) not in ('table', 'incremental'):
        return ''

    dist_key = model_config.get('dist')

    if not isinstance(dist_key, basestring):
        compiler_error(
            model,
            "The provided distkey '{}' is not valid!".format(dist_key)
        )

    dist_key = dist_key.strip().lower()

    adapter = get_adapter(project.run_environment())
    return adapter.dist_qualifier(dist_key)


def get_hooks(model, context, hook_key):
    hooks = model.get('config', {}).get(hook_key, [])

    if isinstance(hooks, basestring):
        hooks = [hooks]

    return hooks


class DatabaseWrapper(object):
    """
    Wrapper for runtime database interaction. Should only call adapter
    functions.
    """

    context_functions = [
        "already_exists",
        "get_columns_in_table",
        "get_missing_columns",
        "query_for_existing",
        "rename",
        "drop",
        "truncate",
        "add_query",
        "expand_target_column_types",
        "commit",
        "get_status",
    ]

    def __init__(self, model, adapter, profile):
        self.model = model
        self.adapter = adapter
        self.profile = profile

    @property
    def raw(self):
        return self.adapter

    def get_context_functions(self):
        return {name: getattr(self, name) for name in self.context_functions}

    def already_exists(self, schema, table):
        return self.adapter.already_exists(
            self.profile, schema, table, self.model.get('name'))

    def get_columns_in_table(self, schema_name, table_name):
        return self.adapter.get_columns_in_table(
            self.profile, schema_name, table_name, self.model.get('name'))

    def get_missing_columns(self, from_schema, from_table,
                            to_schema, to_table):
        return self.adapter.get_missing_columns(
            self.profile, from_schema, from_table,
            to_schema, to_table, self.model.get('name'))

    def query_for_existing(self, schema):
        return self.adapter.query_for_existing(
            self.profile, schema, self.model.get('name'))

    def rename(self, from_name, to_name):
        return self.adapter.rename(
            self.profile, from_name, to_name, self.model.get('name'))

    def drop(self, relation, relation_type):
        return self.adapter.drop(
            self.profile, relation, relation_type, self.model.get('name'))

    def truncate(self, table):
        return self.adapter.truncate(
            self.profile, table, self.model.get('name'))

    def add_query(self, sql, auto_begin=True):
        return self.adapter.add_query(
            self.profile, sql, self.model.get('name'), auto_begin)

    def expand_target_column_types(self, temp_table, to_schema, to_table):
        return self.adapter.expand_target_column_types(
            self.profile, temp_table, to_schema, to_table,
            self.model.get('name'))

    def commit(self):
        return self.adapter.commit_if_has_connection(
            self.profile, self.model.get('name'))

    def get_status(self, cursor):
        return self.adapter.get_status(cursor)


def ref(model, project, schema, flat_graph):
    current_project = project.cfg.get('name')

    def do_ref(*args):
        target_model_name = None
        target_model_package = None

        if len(args) == 1:
            target_model_name = args[0]
        elif len(args) == 2:
            target_model_package, target_model_name = args
        else:
            dbt.exceptions.ref_invalid_args(model, args)

        target_model = dbt.parser.resolve_ref(
            flat_graph,
            target_model_name,
            target_model_package,
            current_project,
            model.get('package_name'))

        if target_model is None:
            dbt.exceptions.ref_target_not_found(
                model,
                target_model_name,
                target_model_package)

        target_model_id = target_model.get('unique_id')

        if target_model_id not in model.get('depends_on', {}).get('nodes'):
            dbt.exceptions.ref_bad_context(model,
                                           target_model_name,
                                           target_model_package)

        if get_materialization(target_model) == 'ephemeral':
            model['extra_ctes'][target_model_id] = None
            return '__dbt__CTE__{}'.format(target_model.get('name'))
        else:
            profile = project.run_environment()
            adapter = get_adapter(profile)
            table = target_model.get('name')
            return adapter.quote_schema_and_table(profile, schema, table)

    return do_ref


def model_config(model):
    def do_config(*args, **kwargs):
        return ''

    return do_config


def generate(model, project, flat_graph):
    context = project.context()
    profile = project.run_environment()
    adapter = get_adapter(profile)

    schema = context['env'].get('schema', 'public')

    # these are empty strings if configs aren't provided
    dist_qualifier = get_dist_qualifier(model, project)
    sort_qualifier = get_sort_qualifier(model, project)

    pre_hooks = get_hooks(model, context, 'pre-hook')
    post_hooks = get_hooks(model, context, 'post-hook')

    rendered_query = model['injected_sql']

    db_wrapper = DatabaseWrapper(model, adapter, profile)

    context = dbt.utils.deep_merge(context, {
        "model": model,
        "this": dbt.utils.This(
            schema,
            dbt.utils.model_immediate_name(model, dbt.flags.NON_DESTRUCTIVE),
            model.get('name')
        ),
        "ref": ref(model, project, schema, flat_graph),
        "var": dbt.utils.Var(model, context=context),
        "config": model_config(model),
        "schema": schema,
        "dist": dist_qualifier,
        "sort": sort_qualifier,
        "pre_hooks": pre_hooks,
        "post_hooks": post_hooks,
        "sql": rendered_query,
        "flags": dbt.flags,
        "adapter": db_wrapper,
        "execute": True,
        "profile": project.run_environment(),
        "log": logger.debug,
        "sql_now": adapter.date_function(),
        "run_started_at": dbt.tracking.active_user.run_started_at,
        "invocation_id": dbt.tracking.active_user.invocation_id,
        "target": project.get_target(),
    }, db_wrapper.get_context_functions())

    for unique_id, macro in flat_graph.get('macros').items():
        package_name = macro.get('package_name')

        macro_map = {
            macro.get('name'): macro.get('generator')(context)
        }

        if context.get(package_name) is None:
            context[package_name] = {}

        context.get(package_name, {}) \
               .update(macro_map)

        if(package_name == model.get('package_name') or
           package_name == dbt.include.GLOBAL_PROJECT_NAME):
            context.update(macro_map)

    return context
