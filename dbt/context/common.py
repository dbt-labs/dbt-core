from dbt.utils import get_materialization, compiler_error
from dbt.adapters.factory import get_adapter
from dbt.compat import basestring

import dbt.clients.jinja
import dbt.flags
import dbt.tracking

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


def get_sort_qualifier(model, profile):
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

    adapter = get_adapter(profile)
    return adapter.sort_qualifier(sort_type, sort_keys)


def get_dist_qualifier(model, profile):
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

    adapter = get_adapter(profile)
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

    def __init__(self, model, adapter, profile):
        self.model = model
        self.adapter = adapter
        self.profile = profile

    @property
    def raw(self):
        return self.adapter

    def wrap_with_profile_and_model_name(fn):
        def wrapped(self, *args, **kwargs):
            args = (self.profile,) + args
            kwargs['model_name'] = self.model.get('name')
            return getattr(self.adapter, fn)(*args, **kwargs)

        return wrapped

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
    ]

    # Fun with metaprogramming
    # Most adapter functions take `profile` as the first argument, and
    # `model_name` as the last. This automatically injects those arguments.
    # In model code, these functions can be called without those two arguments.
    for context_function in context_functions:
        locals()[context_function] = wrap_with_profile_and_model_name(
            context_function)

    def get_status(self, cursor):
        return self.adapter.get_status(cursor)

    def commit(self):
        return self.adapter.commit_if_has_connection(
            self.profile, self.model.get('name'))


def _add_macros(context, model, flat_graph):
    for unique_id, macro in flat_graph.get('macros', {}).items():
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


def _add_tracking(context):
    if dbt.tracking.active_user is not None:
        context = dbt.utils.deep_merge(context, {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
        })
    else:
        context = dbt.utils.deep_merge(context, {
            "run_started_at": None,
            "invocation_id": None
        })

    return context


def generate(model, project, flat_graph, provider=None):
    """
    Not meant to be called directly. Call with either:
        dbt.context.parser.generate
    or
        dbt.context.runtime.generate
    """
    if provider is None:
        raise dbt.exceptions.InternalException(
            "Invalid provider given to context: {}".format(provider))

    target_name = project.get('target')
    profile = project.get('outputs').get(target_name)
    target = profile.copy()
    target['name'] = target_name
    adapter = get_adapter(profile)

    context = {'env': profile}
    schema = profile.get('schema', 'public')

    # these are empty strings if configs aren't provided
    dist_qualifier = get_dist_qualifier(model, profile)
    sort_qualifier = get_sort_qualifier(model, profile)

    pre_hooks = get_hooks(model, context, 'pre-hook')
    post_hooks = get_hooks(model, context, 'post-hook')

    db_wrapper = DatabaseWrapper(model, adapter, profile)

    context = dbt.utils.deep_merge(context, {
        "model": model,
        "this": dbt.utils.This(
            schema,
            dbt.utils.model_immediate_name(model, dbt.flags.NON_DESTRUCTIVE),
            model.get('name')
        ),
        "ref": provider.ref(model, project, profile, schema, flat_graph),
        "var": dbt.utils.Var(model, context=context),
        "config": provider.config(model),
        "schema": schema,
        "dist": dist_qualifier,
        "sort": sort_qualifier,
        "pre_hooks": pre_hooks,
        "post_hooks": post_hooks,
        "sql": model.get('injected_sql'),
        "flags": dbt.flags,
        "adapter": db_wrapper,
        "execute": True,
        "profile": profile,
        "log": logger.debug,
        "sql_now": adapter.date_function(),
        "target": target,
    })

    context = _add_tracking(context)
    context = _add_macros(context, model, flat_graph)

    return context
