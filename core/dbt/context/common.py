import copy
import functools
import json
import os

from dbt.adapters.factory import get_adapter
from dbt.compat import basestring
from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import ParsedMacro, ParsedNode
from dbt.include.global_project import PACKAGES
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME

import dbt.clients.jinja
import dbt.clients.agate_helper
import dbt.flags
import dbt.schema
import dbt.tracking
import dbt.utils

import dbt.hooks

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime


class RelationProxy(object):
    def __init__(self, adapter):
        self.quoting_config = adapter.config.quoting
        self.relation_type = adapter.Relation

    def __getattr__(self, key):
        return getattr(self.relation_type, key)

    def create_from_source(self, *args, **kwargs):
        # bypass our create when creating from source so as not to mess up
        # the source quoting
        return self.relation_type.create_from_source(*args, **kwargs)

    def create(self, *args, **kwargs):
        kwargs['quote_policy'] = dbt.utils.merge(
            self.quoting_config,
            kwargs.pop('quote_policy', {})
        )
        return self.relation_type.create(*args, **kwargs)


class DatabaseWrapper(object):
    """
    Wrapper for runtime database interaction. Mostly a compatibility layer now.
    """
    def __init__(self, connection_name, adapter):
        self.connection_name = connection_name
        self.adapter = adapter
        self.Relation = RelationProxy(adapter)

    def wrap(self, name):
        func = getattr(self.adapter, name)

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            kwargs['model_name'] = self.connection_name
            return func(*args, **kwargs)

        return wrapped

    def __getattr__(self, name):
        if name in self.adapter._available_model_:
            return self.wrap(name)
        elif name in self.adapter._available_raw_:
            return getattr(self.adapter, name)
        else:
            raise AttributeError(
                "'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, name
                )
            )

    @property
    def config(self):
        return self.adapter.config

    def type(self):
        return self.adapter.type()

    def commit(self):
        return self.adapter.commit_if_has_connection(self.connection_name)


def _add_macro_map(context, package_name, macro_map):
    """Update an existing context in-place, adding the given macro map to the
    appropriate package namespace. Adapter packages get inserted into the
    global namespace.
    """
    key = package_name
    if package_name in PACKAGES:
        key = GLOBAL_PROJECT_NAME
    if key not in context:
        context[key] = {}

    context[key].update(macro_map)


def _add_macros(context, model, manifest):
    macros_to_add = {'global': [], 'local': []}

    for unique_id, macro in manifest.macros.items():
        if macro.resource_type != NodeType.Macro:
            continue
        package_name = macro.package_name

        macro_map = {
            macro.name: macro.generator(context)
        }

        # adapter packages are part of the global project space
        _add_macro_map(context, package_name, macro_map)

        if package_name == model.package_name:
            macros_to_add['local'].append(macro_map)
        elif package_name in PACKAGES:
            macros_to_add['global'].append(macro_map)

    # Load global macros before local macros -- local takes precedence
    unprefixed_macros = macros_to_add['global'] + macros_to_add['local']
    for macro_map in unprefixed_macros:
        context.update(macro_map)

    return context


def _add_tracking(context):
    if dbt.tracking.active_user is not None:
        context = dbt.utils.merge(context, {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
        })
    else:
        context = dbt.utils.merge(context, {
            "run_started_at": None,
            "invocation_id": None
        })

    return context


def _add_validation(context):
    def validate_any(*args):
        def inner(value):
            for arg in args:
                if isinstance(arg, type) and isinstance(value, arg):
                    return
                elif value == arg:
                    return
            raise dbt.exceptions.ValidationException(
                'Expected value "{}" to be one of {}'
                .format(value, ','.join(map(str, args))))
        return inner

    validation_utils = dbt.utils.AttrDict({
        'any': validate_any,
    })

    return dbt.utils.merge(
        context,
        {'validation': validation_utils})


def env_var(var, default=None):
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = "Env var required but not provided: '{}'".format(var)
        dbt.clients.jinja.undefined_error(msg)


def _store_result(sql_results):
    def call(name, status, agate_table=None):
        if agate_table is None:
            agate_table = dbt.clients.agate_helper.empty_table()

        sql_results[name] = dbt.utils.AttrDict({
            'status': status,
            'data': dbt.clients.agate_helper.as_matrix(agate_table),
            'table': agate_table
        })
        return ''

    return call


def _load_result(sql_results):
    def call(name):
        return sql_results.get(name)

    return call


def _add_sql_handlers(context):
    sql_results = {}
    return dbt.utils.merge(context, {
        '_sql_results': sql_results,
        'store_result': _store_result(sql_results),
        'load_result': _load_result(sql_results),
    })


def log(msg, info=False):
    if info:
        logger.info(msg)
    else:
        logger.debug(msg)
    return ''


class Var(object):
    UndefinedVarError = "Required var '{}' not found in config:\nVars "\
                        "supplied to {} = {}"
    NoneVarError = "Supplied var '{}' is undefined in config:\nVars supplied "\
                   "to {} = {}"

    def __init__(self, model, context, overrides):
        self.model = model
        self.context = context

        # These are hard-overrides (eg. CLI vars) that should take
        # precedence over context-based var definitions
        self.overrides = overrides

        if isinstance(model, dict) and model.get('unique_id'):
            local_vars = model.get('config', {}).get('vars', {})
            self.model_name = model.get('name')
        elif isinstance(model, ParsedMacro):
            local_vars = {}  # macros have no config
            self.model_name = model.name
        elif isinstance(model, ParsedNode):
            local_vars = model.config.get('vars', {})
            self.model_name = model.name
        elif model is None:
            # during config parsing we have no model and no local vars
            self.model_name = '<Configuration>'
            local_vars = {}
        else:
            # still used for wrapping
            self.model_name = model.nice_name
            local_vars = model.config.get('vars', {})

        self.local_vars = dbt.utils.merge(local_vars, overrides)

    def pretty_dict(self, data):
        return json.dumps(data, sort_keys=True, indent=4)

    def assert_var_defined(self, var_name, default):
        if var_name not in self.local_vars and default is None:
            pretty_vars = self.pretty_dict(self.local_vars)
            dbt.exceptions.raise_compiler_error(
                self.UndefinedVarError.format(
                    var_name, self.model_name, pretty_vars
                ),
                self.model
            )

    def assert_var_not_none(self, var_name):
        raw = self.local_vars[var_name]
        if raw is None:
            pretty_vars = self.pretty_dict(self.local_vars)
            dbt.exceptions.raise_compiler_error(
                self.NoneVarError.format(
                    var_name, self.model_name, pretty_vars
                ),
                self.model
            )

    def __call__(self, var_name, default=None):
        self.assert_var_defined(var_name, default)

        if var_name not in self.local_vars:
            return default

        self.assert_var_not_none(var_name)

        raw = self.local_vars[var_name]

        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(raw, basestring):
            return raw

        return dbt.clients.jinja.get_rendered(raw, self.context)


def write(node, target_path, subdirectory):
    def fn(payload):
        node['build_path'] = dbt.writer.write_node(
            node, target_path, subdirectory, payload)
        return ''

    return fn


def render(context, node):
    def fn(string):
        return dbt.clients.jinja.get_rendered(string, context, node)

    return fn


def fromjson(string, default=None):
    try:
        return json.loads(string)
    except ValueError as e:
        return default


def tojson(value, default=None):
    try:
        return json.dumps(value)
    except ValueError as e:
        return default


def try_or_compiler_error(model):
    def impl(message_if_exception, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            dbt.exceptions.raise_compiler_error(message_if_exception, model)
    return impl


def _return(value):
    raise dbt.exceptions.MacroReturn(value)


def get_this_relation(db_wrapper, config, model):
    return db_wrapper.Relation.create_from_node(config, model)


def get_pytz_module_context():
    context_exports = pytz.__all__

    return {
        name: getattr(pytz, name) for name in context_exports
    }


def get_datetime_module_context():
    context_exports = [
        'date',
        'datetime',
        'time',
        'timedelta',
        'tzinfo'
    ]

    return {
        name: getattr(datetime, name) for name in context_exports
    }


def generate_base(model, model_dict, config, manifest, source_config,
                  provider, connection_name):
    """Generate the common aspects of the config dict."""
    if provider is None:
        raise dbt.exceptions.InternalException(
            "Invalid provider given to context: {}".format(provider))

    target_name = config.target_name
    target = config.to_profile_info()
    del target['credentials']
    target.update(config.credentials.serialize(with_aliases=True))
    target['type'] = config.credentials.type
    target.pop('pass', None)
    target['name'] = target_name
    adapter = get_adapter(config)

    context = {'env': target}

    pre_hooks = None
    post_hooks = None

    db_wrapper = DatabaseWrapper(connection_name, adapter)

    context = dbt.utils.merge(context, {
        "adapter": db_wrapper,
        "api": {
            "Relation": db_wrapper.Relation,
            "Column": adapter.Column,
        },
        "column": adapter.Column,
        "config": provider.Config(model_dict, source_config),
        "database": config.credentials.database,
        "env_var": env_var,
        "exceptions": dbt.exceptions.CONTEXT_EXPORTS,
        "execute": provider.execute,
        "flags": dbt.flags,
        # TODO: Do we have to leave this in?
        "graph": manifest.to_flat_graph(),
        "log": log,
        "model": model_dict,
        "modules": {
            "pytz": get_pytz_module_context(),
            "datetime": get_datetime_module_context(),
        },
        "post_hooks": post_hooks,
        "pre_hooks": pre_hooks,
        "ref": provider.ref(db_wrapper, model, config, manifest),
        "return": _return,
        "schema": config.credentials.schema,
        "sql": None,
        "sql_now": adapter.date_function(),
        "source": provider.source(db_wrapper, model, config, manifest),
        "fromjson": fromjson,
        "tojson": tojson,
        "target": target,
        "try_or_compiler_error": try_or_compiler_error(model)
    })

    return context


def modify_generated_context(context, model, model_dict, config, manifest):
    cli_var_overrides = config.cli_vars

    context = _add_tracking(context)
    context = _add_validation(context)
    context = _add_sql_handlers(context)

    # we make a copy of the context for each of these ^^

    context = _add_macros(context, model, manifest)

    context["write"] = write(model_dict, config.target_path, 'run')
    context["render"] = render(context, model_dict)
    context["var"] = Var(model, context=context, overrides=cli_var_overrides)
    context['context'] = context

    return context


def generate_execute_macro(model, config, manifest, provider, connection_name):
    """Internally, macros can be executed like nodes, with some restrictions:

     - they don't have have all values available that nodes do:
        - 'this', 'pre_hooks', 'post_hooks', and 'sql' are missing
        - 'schema' does not use any 'model' information
     - they can't be configured with config() directives
    """
    model_dict = model.serialize()
    context = generate_base(model, model_dict, config, manifest,
                            None, provider, connection_name)

    return modify_generated_context(context, model, model_dict, config,
                                    manifest)


def generate_model(model, config, manifest, source_config, provider):
    model_dict = model.to_dict()
    context = generate_base(model, model_dict, config, manifest,
                            source_config, provider, model.get('name'))
    # operations (hooks) don't get a 'this'
    if model.resource_type != NodeType.Operation:
        this = get_this_relation(context['adapter'], config, model_dict)
        context['this'] = this
    # overwrite schema/database if we have them, and hooks + sql
    context.update({
        'schema': model.get('schema', context['schema']),
        'database': model.get('database', context['database']),
        'pre_hooks': model.config.get('pre-hook'),
        'post_hooks': model.config.get('post-hook'),
        'sql': model.get('injected_sql'),
    })

    return modify_generated_context(context, model, model_dict, config,
                                    manifest)


def generate(model, config, manifest, source_config=None, provider=None):
    """
    Not meant to be called directly. Call with either:
        dbt.context.parser.generate
    or
        dbt.context.runtime.generate
    """
    return generate_model(model, config, manifest, source_config,
                          provider)
