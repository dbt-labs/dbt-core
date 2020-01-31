import json
import os
from typing import (
    Any, Dict, NoReturn, Optional
)

from dbt import flags
from dbt import tracking
from dbt.clients.jinja import undefined_error, get_rendered
from dbt.exceptions import raise_compiler_error, MacroReturn
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import merge
from dbt.version import __version__ as dbt_version

import yaml
# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime


def get_pytz_module_context() -> Dict[str, Any]:
    context_exports = pytz.__all__  # type: ignore

    return {
        name: getattr(pytz, name) for name in context_exports
    }


def get_datetime_module_context() -> Dict[str, Any]:
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


def get_context_modules() -> Dict[str, Dict[str, Any]]:
    return {
        'pytz': get_pytz_module_context(),
        'datetime': get_datetime_module_context(),
    }


class ContextMember:
    def __init__(self, value, name=None):
        self.name = name
        self.inner = value

    def key(self, default):
        if self.name is None:
            return default
        return self.name


def contextmember(value):
    if isinstance(value, str):
        return lambda v: ContextMember(v, name=value)
    return ContextMember(value)


def contextproperty(value):
    if isinstance(value, str):
        return lambda v: ContextMember(property(v), name=value)
    return ContextMember(property(value))


class ContextMeta(type):
    def __new__(mcls, name, bases, dct):
        context_members = {}
        context_attrs = {}
        new_dct = {}

        for base in bases:
            context_members.update(getattr(base, '_context_members_', {}))
            context_attrs.update(getattr(base, '_context_attrs_', {}))

        for key, value in dct.items():
            if isinstance(value, ContextMember):
                context_key = value.key(key)
                context_members[context_key] = value.inner
                context_attrs[context_key] = key
                value = value.inner
            new_dct[key] = value
        new_dct['_context_members_'] = context_members
        new_dct['_context_attrs_'] = context_attrs
        return type.__new__(mcls, name, bases, new_dct)


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

        self.local_vars = merge(local_vars, overrides)

    def pretty_dict(self, data):
        return json.dumps(data, sort_keys=True, indent=4)

    def get_missing_var(self, var_name):
        pretty_vars = self.pretty_dict(self.local_vars)
        msg = self.UndefinedVarError.format(
            var_name, self.model_name, pretty_vars
        )
        raise_compiler_error(msg, self.model)

    def assert_var_defined(self, var_name, default):
        if var_name not in self.local_vars and default is self._VAR_NOTSET:
            return self.get_missing_var(var_name)

    def get_rendered_var(self, var_name):
        raw = self.local_vars[var_name]
        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(raw, str):
            return raw

        return get_rendered(raw, self.context)

    def __call__(self, var_name, default=_VAR_NOTSET):
        if var_name in self.local_vars:
            return self.get_rendered_var(var_name)
        elif default is not self._VAR_NOTSET:
            return default
        else:
            return self.get_missing_var(var_name)


class BaseContext(metaclass=ContextMeta):
    def __init__(self, cli_vars):
        self._ctx = {}
        self.cli_vars = cli_vars

    def generate_builtins(self):
        builtins: Dict[str, Any] = {}
        for key, value in self._context_members_.items():
            if hasattr(value, '__get__'):
                # handle properties, bound methods, etc
                value = value.__get__(self)
            builtins[key] = value
        return builtins

    def to_dict(self):
        self._ctx['context'] = self._ctx
        builtins = self.generate_builtins()
        self._ctx['builtins'] = builtins
        self._ctx.update(builtins)
        return self._ctx

    @contextproperty
    def dbt_version(self) -> str:
        return dbt_version

    @contextproperty
    def var(self) -> Var:
        return Var(None, self._ctx, self.cli_vars)

    @contextmember
    @staticmethod
    def env_var(var: str, default: Optional[str] = None) -> str:
        if var in os.environ:
            return os.environ[var]
        elif default is not None:
            return default
        else:
            msg = f"Env var required but not provided: '{var}'"
            undefined_error(msg)

    if os.environ.get('DBT_MACRO_DEBUGGING'):
        @contextmember
        @staticmethod
        def debug():
            import sys
            import ipdb  # type: ignore
            frame = sys._getframe(3)
            ipdb.set_trace(frame)
            return ''

    @contextmember('return')
    @staticmethod
    def _return(value: Any) -> NoReturn:
        raise MacroReturn(value)

    @contextmember
    @staticmethod
    def fromjson(string: str, default: Any = None) -> Any:
        try:
            return json.loads(string)
        except ValueError:
            return default

    @contextmember
    @staticmethod
    def tojson(
        value: Any, default: Any = None, sort_keys: bool = False
    ) -> Any:
        try:
            return json.dumps(value, sort_keys=sort_keys)
        except ValueError:
            return default

    @contextmember
    @staticmethod
    def fromyaml(value: str, default: Any = None) -> Any:
        try:
            return yaml.safe_load(value)
        except (AttributeError, ValueError, yaml.YAMLError):
            return default

    # safe_dump defaults to sort_keys=True, but we act like json.dumps (the
    # opposite)
    @contextmember
    @staticmethod
    def toyaml(
        value: Any, default: Optional[str] = None, sort_keys: bool = False
    ) -> Optional[str]:
        try:
            return yaml.safe_dump(data=value, sort_keys=sort_keys)
        except (ValueError, yaml.YAMLError):
            return default

    @contextmember
    @staticmethod
    def log(msg: str, info: bool = False) -> str:
        if info:
            logger.info(msg)
        else:
            logger.debug(msg)
        return ''

    @contextproperty
    def run_started_at(self) -> Optional[datetime.datetime]:
        if tracking.active_user is not None:
            return tracking.active_user.run_started_at
        else:
            return None

    @contextproperty
    def invocation_id(self) -> Optional[str]:
        if tracking.active_user is not None:
            return tracking.active_user.invocation_id
        else:
            return None

    @contextproperty
    def modules(self) -> Dict[str, Any]:
        return get_context_modules()

    @contextproperty
    def flags(self) -> Any:
        return flags


def generate_base_context(cli_vars: Dict[str, Any]) -> Dict[str, Any]:
    ctx = BaseContext(cli_vars)
    return ctx.to_dict()
