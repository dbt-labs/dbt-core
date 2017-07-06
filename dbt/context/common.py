import voluptuous

from dbt.utils import get_materialization, compiler_error
from dbt.adapters.factory import get_adapter
from dbt.compat import basestring

import dbt.clients.jinja
import dbt.flags
import dbt.schema
import dbt.tracking

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


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

        # Fun with metaprogramming
        # Most adapter functions take `profile` as the first argument, and
        # `model_name` as the last. This automatically injects those arguments.
        # In model code, these functions can be called without those two args.
        for context_function in self.adapter.context_functions:
            setattr(self,
                    context_function,
                    self.wrap_with_profile_and_model_name(context_function))

        for raw_function in self.adapter.raw_functions:
            setattr(self,
                    raw_function,
                    getattr(self.adapter, raw_function))

    def wrap_with_profile_and_model_name(self, fn):
        def wrapped(*args, **kwargs):
            args = (self.profile,) + args
            kwargs['model_name'] = self.model.get('name')
            return getattr(self.adapter, fn)(*args, **kwargs)

        return wrapped

    def type(self):
        return self.adapter.type()

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


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def _add_validation(context):
    validation_utils = AttrDict({
        'any': voluptuous.Any,
        'all': voluptuous.All,
    })

    return dbt.utils.deep_merge(
        context,
        {'validation': validation_utils})


def log(msg):
    logger.debug(msg)
    return ''


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
    target.pop('pass', None)
    target['name'] = target_name
    adapter = get_adapter(profile)

    context = {'env': target}
    schema = profile.get('schema', 'public')

    pre_hooks = get_hooks(model, context, 'pre-hook')
    post_hooks = get_hooks(model, context, 'post-hook')

    db_wrapper = DatabaseWrapper(model, adapter, profile)

    context = dbt.utils.deep_merge(context, {
        "adapter": db_wrapper,
        "column": dbt.schema.Column,
        "config": provider.Config(model),
        "execute": True,
        "flags": dbt.flags,
        "graph": flat_graph,
        "log": log,
        "model": model,
        "post_hooks": post_hooks,
        "pre_hooks": pre_hooks,
        "ref": provider.ref(model, project, profile, schema, flat_graph),
        "schema": schema,
        "sql": model.get('injected_sql'),
        "sql_now": adapter.date_function(),
        "target": target,
        "this": dbt.utils.This(
            schema,
            dbt.utils.model_immediate_name(model, dbt.flags.NON_DESTRUCTIVE),
            model.get('name')
        ),
        "var": dbt.utils.Var(model, context=context),
    })

    context = _add_tracking(context)
    context = _add_macros(context, model, flat_graph)
    context = _add_validation(context)

    context['context'] = context

    return context
