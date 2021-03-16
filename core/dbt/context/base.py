import json
import os
from typing import (
    Any, Dict, NoReturn, Optional, Mapping
)

from dbt import flags
from dbt import tracking
from dbt.clients.jinja import undefined_error, get_rendered
from dbt.clients.yaml_helper import (  # noqa: F401
    yaml, safe_load, SafeLoader, Loader, Dumper
)
from dbt.contracts.graph.compiled import CompiledResource
from dbt.exceptions import raise_compiler_error, MacroReturn
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.version import __version__ as dbt_version

# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime
import re


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


def get_re_module_context() -> Dict[str, Any]:
    context_exports = re.__all__

    return {
        name: getattr(re, name) for name in context_exports
    }


def get_context_modules() -> Dict[str, Dict[str, Any]]:
    return {
        'pytz': get_pytz_module_context(),
        'datetime': get_datetime_module_context(),
        're': get_re_module_context(),
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

    def __init__(
        self,
        context: Mapping[str, Any],
        cli_vars: Mapping[str, Any],
        node: Optional[CompiledResource] = None
    ) -> None:
        self._context: Mapping[str, Any] = context
        self._cli_vars: Mapping[str, Any] = cli_vars
        self._node: Optional[CompiledResource] = node
        self._merged: Mapping[str, Any] = self._generate_merged()

    def _generate_merged(self) -> Mapping[str, Any]:
        return self._cli_vars

    @property
    def node_name(self):
        if self._node is not None:
            return self._node.name
        else:
            return '<Configuration>'

    def get_missing_var(self, var_name):
        dct = {k: self._merged[k] for k in self._merged}
        pretty_vars = json.dumps(dct, sort_keys=True, indent=4)
        msg = self.UndefinedVarError.format(
            var_name, self.node_name, pretty_vars
        )
        raise_compiler_error(msg, self._node)

    def has_var(self, var_name: str):
        return var_name in self._merged

    def get_rendered_var(self, var_name):
        raw = self._merged[var_name]
        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(raw, str):
            return raw

        return get_rendered(raw, self._context)

    def __call__(self, var_name, default=_VAR_NOTSET):
        if self.has_var(var_name):
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

    # no dbtClassMixin so this is not an actual override
    def to_dict(self):
        self._ctx['context'] = self._ctx
        builtins = self.generate_builtins()
        self._ctx['builtins'] = builtins
        self._ctx.update(builtins)
        return self._ctx

    @contextproperty
    def dbt_version(self) -> str:
        """The `dbt_version` variable returns the installed version of dbt that
        is currently running. It can be used for debugging or auditing
        purposes.

        > macros/get_version.sql

            {% macro get_version() %}
              {% set msg = "The installed version of dbt is: " ~ dbt_version %}
              {% do log(msg, info=true) %}
            {% endmacro %}

        Example output:

            $ dbt run-operation get_version
            The installed version of dbt is 0.16.0
        """
        return dbt_version

    @contextproperty
    def var(self) -> Var:
        """Variables can be passed from your `dbt_project.yml` file into models
        during compilation. These variables are useful for configuring packages
        for deployment in multiple environments, or defining values that should
        be used across multiple models within a package.

        To add a variable to a model, use the `var()` function:

        > my_model.sql:

            select * from events where event_type = '{{ var("event_type") }}'

        If you try to run this model without supplying an `event_type`
        variable, you'll receive a compilation error that looks like this:

            Encountered an error:
            ! Compilation error while compiling model package_name.my_model:
            ! Required var 'event_type' not found in config:
            Vars supplied to package_name.my_model = {
            }

        To supply a variable to a given model, add one or more `vars`
        dictionaries to the `models` config in your `dbt_project.yml` file.
        These `vars` are in-scope for all models at or below where they are
        defined, so place them where they make the most sense. Below are three
        different placements of the `vars` dict, all of which will make the
        `my_model` model compile.

        > dbt_project.yml:

            # 1) scoped at the model level
            models:
              package_name:
                my_model:
                  materialized: view
                  vars:
                    event_type: activation
            # 2) scoped at the package level
            models:
              package_name:
                vars:
                  event_type: activation
                my_model:
                  materialized: view
            # 3) scoped globally
            models:
              vars:
                event_type: activation
              package_name:
                my_model:
                  materialized: view

        ## Variable default values

        The `var()` function takes an optional second argument, `default`. If
        this argument is provided, then it will be the default value for the
        variable if one is not explicitly defined.

        > my_model.sql:

            -- Use 'activation' as the event_type if the variable is not
            -- defined.
            select *
            from events
            where event_type = '{{ var("event_type", "activation") }}'
        """
        return Var(self._ctx, self.cli_vars)

    @contextmember
    @staticmethod
    def env_var(var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.
        """
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
            """Enter a debugger at this line in the compiled jinja code."""
            import sys
            import ipdb  # type: ignore
            frame = sys._getframe(3)
            ipdb.set_trace(frame)
            return ''

    @contextmember('return')
    @staticmethod
    def _return(data: Any) -> NoReturn:
        """The `return` function can be used in macros to return data to the
        caller. The type of the data (`dict`, `list`, `int`, etc) will be
        preserved through the return call.

        :param data: The data to return to the caller


        > macros/example.sql:

            {% macro get_data() %}
              {{ return([1,2,3]) }}
            {% endmacro %}

        > models/my_model.sql:

            select
              -- getdata() returns a list!
              {% for i in getdata() %}
                {{ i }}
                {% if not loop.last %},{% endif %}
              {% endfor %}

        """
        raise MacroReturn(data)

    @contextmember
    @staticmethod
    def fromjson(string: str, default: Any = None) -> Any:
        """The `fromjson` context method can be used to deserialize a json
        string into a Python object primitive, eg. a `dict` or `list`.

        :param value: The json string to deserialize
        :param default: A default value to return if the `string` argument
            cannot be deserialized (optional)

        Usage:

            {% set my_json_str = '{"abc": 123}' %}
            {% set my_dict = fromjson(my_json_str) %}
            {% do log(my_dict['abc']) %}
        """
        try:
            return json.loads(string)
        except ValueError:
            return default

    @contextmember
    @staticmethod
    def tojson(
        value: Any, default: Any = None, sort_keys: bool = False
    ) -> Any:
        """The `tojson` context method can be used to serialize a Python
        object primitive, eg. a `dict` or `list` to a json string.

        :param value: The value serialize to json
        :param default: A default value to return if the `value` argument
            cannot be serialized
        :param sort_keys: If True, sort the keys.


        Usage:

            {% set my_dict = {"abc": 123} %}
            {% set my_json_string = tojson(my_dict) %}
            {% do log(my_json_string) %}
        """
        try:
            return json.dumps(value, sort_keys=sort_keys)
        except ValueError:
            return default

    @contextmember
    @staticmethod
    def fromyaml(value: str, default: Any = None) -> Any:
        """The fromyaml context method can be used to deserialize a yaml string
        into a Python object primitive, eg. a `dict` or `list`.

        :param value: The yaml string to deserialize
        :param default: A default value to return if the `string` argument
            cannot be deserialized (optional)

        Usage:

            {% set my_yml_str -%}
            dogs:
             - good
             - bad
            {%- endset %}
            {% set my_dict = fromyaml(my_yml_str) %}
            {% do log(my_dict['dogs'], info=true) %}
            -- ["good", "bad"]
            {% do my_dict['dogs'].pop() }
            {% do log(my_dict['dogs'], info=true) %}
            -- ["good"]
        """
        try:
            return safe_load(value)
        except (AttributeError, ValueError, yaml.YAMLError):
            return default

    # safe_dump defaults to sort_keys=True, but we act like json.dumps (the
    # opposite)
    @contextmember
    @staticmethod
    def toyaml(
        value: Any, default: Optional[str] = None, sort_keys: bool = False
    ) -> Optional[str]:
        """The `tojson` context method can be used to serialize a Python
        object primitive, eg. a `dict` or `list` to a yaml string.

        :param value: The value serialize to yaml
        :param default: A default value to return if the `value` argument
            cannot be serialized
        :param sort_keys: If True, sort the keys.


        Usage:

            {% set my_dict = {"abc": 123} %}
            {% set my_yaml_string = toyaml(my_dict) %}
            {% do log(my_yaml_string) %}
        """
        try:
            return yaml.safe_dump(data=value, sort_keys=sort_keys)
        except (ValueError, yaml.YAMLError):
            return default

    @contextmember
    @staticmethod
    def log(msg: str, info: bool = False) -> str:
        """Logs a line to either the log file or stdout.

        :param msg: The message to log
        :param info: If `False`, write to the log file. If `True`, write to
            both the log file and stdout.

        > macros/my_log_macro.sql

            {% macro some_macro(arg1, arg2) %}
              {{ log("Running some_macro: " ~ arg1 ~ ", " ~ arg2) }}
            {% endmacro %}"
        """
        if info:
            logger.info(msg)
        else:
            logger.debug(msg)
        return ''

    @contextproperty
    def run_started_at(self) -> Optional[datetime.datetime]:
        """`run_started_at` outputs the timestamp that this run started, e.g.
        `2017-04-21 01:23:45.678`. The `run_started_at` variable is a Python
        `datetime` object. As of 0.9.1, the timezone of this variable defaults
        to UTC.

        > run_started_at_example.sql

            select
                '{{ run_started_at.strftime("%Y-%m-%d") }}' as date_day
            from ...


        To modify the timezone of this variable, use the the `pytz` module:

        > run_started_at_utc.sql

            {% set est = modules.pytz.timezone("America/New_York") %}
            select
                '{{ run_started_at.astimezone(est) }}' as run_started_est
            from ...
        """
        if tracking.active_user is not None:
            return tracking.active_user.run_started_at
        else:
            return None

    @contextproperty
    def invocation_id(self) -> Optional[str]:
        """invocation_id outputs a UUID generated for this dbt run (useful for
        auditing)
        """
        if tracking.active_user is not None:
            return tracking.active_user.invocation_id
        else:
            return None

    @contextproperty
    def modules(self) -> Dict[str, Any]:
        """The `modules` variable in the Jinja context contains useful Python
        modules for operating on data.

        # datetime

        This variable is a pointer to the Python datetime module.

        Usage:

            {% set dt = modules.datetime.datetime.now() %}

        # pytz

        This variable is a pointer to the Python pytz module.

        Usage:

            {% set dt = modules.datetime.datetime(2002, 10, 27, 6, 0, 0) %}
            {% set dt_local = modules.pytz.timezone('US/Eastern').localize(dt) %}
            {{ dt_local }}
        """  # noqa
        return get_context_modules()

    @contextproperty
    def flags(self) -> Any:
        """The `flags` variable contains true/false values for flags provided
        on the command line.

        > flags.sql:

            {% if flags.FULL_REFRESH %}
            drop table ...
            {% else %}
            -- no-op
            {% endif %}

        The list of valid flags are:

        - `flags.STRICT_MODE`: True if `--strict` (or `-S`) was provided on the
            command line
        - `flags.FULL_REFRESH`: True if `--full-refresh` was provided on the
            command line
        - `flags.NON_DESTRUCTIVE`: True if `--non-destructive` was provided on
            the command line
        """
        return flags


def generate_base_context(cli_vars: Dict[str, Any]) -> Dict[str, Any]:
    ctx = BaseContext(cli_vars)
    # This is not a Mashumaro to_dict call
    return ctx.to_dict()
