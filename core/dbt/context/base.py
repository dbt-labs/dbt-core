import json
import os

import dbt.tracking
from dbt.clients.jinja import undefined_error
from dbt.utils import merge


# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime


def add_tracking(context):
    if dbt.tracking.active_user is not None:
        context = merge(context, {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
        })
    else:
        context = merge(context, {
            "run_started_at": None,
            "invocation_id": None
        })

    return context


def env_var(var, default=None):
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = "Env var required but not provided: '{}'".format(var)
        undefined_error(msg)


def debug_here():
    import sys
    import ipdb  # type: ignore
    frame = sys._getframe(3)
    ipdb.set_trace(frame)


class Var:
    UndefinedVarError = "Required var '{}' not found in config:\nVars "\
                        "supplied to {} = {}"
    _VAR_NOTSET = object()

    def __init__(self, model, context, overrides):
        self.model = model
        self.context = context

        # These are hard-overrides (eg. CLI vars) that should take
        # precedence over context-based var definitions
        self.overrides = overrides

        if model is None:
            # during config parsing we have no model and no local vars
            self.model_name = '<Configuration>'
            local_vars = {}
        else:
            self.model_name = model.name
            local_vars = model.local_vars()

        self.local_vars = dbt.utils.merge(local_vars, overrides)

    def pretty_dict(self, data):
        return json.dumps(data, sort_keys=True, indent=4)

    def get_missing_var(self, var_name):
        pretty_vars = self.pretty_dict(self.local_vars)
        msg = self.UndefinedVarError.format(
            var_name, self.model_name, pretty_vars
        )
        dbt.exceptions.raise_compiler_error(msg, self.model)

    def assert_var_defined(self, var_name, default):
        if var_name not in self.local_vars and default is self._VAR_NOTSET:
            return self.get_missing_var(var_name)

    def get_rendered_var(self, var_name):
        raw = self.local_vars[var_name]
        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(raw, str):
            return raw

        return dbt.clients.jinja.get_rendered(raw, self.context)

    def __call__(self, var_name, default=_VAR_NOTSET):
        if var_name in self.local_vars:
            return self.get_rendered_var(var_name)
        elif default is not self._VAR_NOTSET:
            return default
        else:
            return self.get_missing_var(var_name)


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


def get_context_modules():
    return {
        'pytz': get_pytz_module_context(),
        'datetime': get_datetime_module_context(),
    }


def generate_config_context(cli_vars):
    context = {
        'env_var': env_var,
        'modules': get_context_modules(),
    }
    context['var'] = Var(None, context, cli_vars)
    if os.environ.get('DBT_MACRO_DEBUGGING'):
        context['debug'] = debug_here
    return add_tracking(context)
