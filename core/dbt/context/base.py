import json
import os
from typing import Any, Dict, NoReturn, Optional, Mapping, Iterable, Set

from dbt import flags
from dbt import tracking
from dbt.clients.jinja import get_rendered
from dbt.clients.yaml_helper import yaml, safe_load, SafeLoader, Loader, Dumper  # noqa: F401
from dbt.contracts.graph.compiled import CompiledResource
from dbt.exceptions import (
    CompilationException,
    MacroReturn,
    raise_compiler_error,
    raise_parsing_error,
    disallow_secret_env_var,
)
from dbt.logger import SECRET_ENV_PREFIX
from dbt.events.functions import fire_event, get_invocation_id
from dbt.events.types import MacroEventInfo, MacroEventDebug
from dbt.version import __version__ as dbt_version

# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime
import re
import itertools

# See the `contexts` module README for more information on how contexts work


def get_pytz_module_context() -> Dict[str, Any]:
    context_exports = pytz.__all__  # type: ignore

    return {name: getattr(pytz, name) for name in context_exports}


def get_datetime_module_context() -> Dict[str, Any]:
    context_exports = ["date", "datetime", "time", "timedelta", "tzinfo"]

    return {name: getattr(datetime, name) for name in context_exports}


def get_re_module_context() -> Dict[str, Any]:
    # TODO CT-211
    context_exports = re.__all__  # type: ignore[attr-defined]

    return {name: getattr(re, name) for name in context_exports}


def get_itertools_module_context() -> Dict[str, Any]:
    # Excluded dropwhile, filterfalse, takewhile and groupby;
    # first 3 illogical for Jinja and last redundant.
    context_exports = [
        "count",
        "cycle",
        "repeat",
        "accumulate",
        "chain",
        "compress",
        "islice",
        "starmap",
        "tee",
        "zip_longest",
        "product",
        "permutations",
        "combinations",
        "combinations_with_replacement",
    ]

    return {name: getattr(itertools, name) for name in context_exports}


def get_context_modules() -> Dict[str, Dict[str, Any]]:
    return {
        "pytz": get_pytz_module_context(),
        "datetime": get_datetime_module_context(),
        "re": get_re_module_context(),
        "itertools": get_itertools_module_context(),
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
            context_members.update(getattr(base, "_context_members_", {}))
            context_attrs.update(getattr(base, "_context_attrs_", {}))

        for key, value in dct.items():
            if isinstance(value, ContextMember):
                context_key = value.key(key)
                context_members[context_key] = value.inner
                context_attrs[context_key] = key
                value = value.inner
            new_dct[key] = value
        new_dct["_context_members_"] = context_members
        new_dct["_context_attrs_"] = context_attrs
        return type.__new__(mcls, name, bases, new_dct)


class Var:
    UndefinedVarError = "Required var '{}' not found in config:\nVars " "supplied to {} = {}"
    _VAR_NOTSET = object()

    def __init__(
        self,
        context: Mapping[str, Any],
        cli_vars: Mapping[str, Any],
        node: Optional[CompiledResource] = None,
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
            return "<Configuration>"

    def get_missing_var(self, var_name):
        dct = {k: self._merged[k] for k in self._merged}
        pretty_vars = json.dumps(dct, sort_keys=True, indent=4)
        msg = self.UndefinedVarError.format(var_name, self.node_name, pretty_vars)
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
    # subclass is TargetContext
    def __init__(self, cli_vars):
        self._ctx = {}
        self.cli_vars = cli_vars
        self.env_vars = {}

    def generate_builtins(self):
        builtins: Dict[str, Any] = {}
        for key, value in self._context_members_.items():
            if hasattr(value, "__get__"):
                # handle properties, bound methods, etc
                value = value.__get__(self)
            builtins[key] = value
        return builtins

    # no dbtClassMixin so this is not an actual override
    def to_dict(self):
        self._ctx["context"] = self._ctx
        builtins = self.generate_builtins()
        self._ctx["builtins"] = builtins
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
    def env_var(self, var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.
        """
        return_value = None
        if var.startswith(SECRET_ENV_PREFIX):
            disallow_secret_env_var(var)
        if var in os.environ:
            return_value = os.environ[var]
        elif default is not None:
            return_value = default

        if return_value is not None:
            self.env_vars[var] = return_value
            return return_value
        else:
            msg = f"Env var required but not provided: '{var}'"
            raise_parsing_error(msg)

    if os.environ.get("DBT_MACRO_DEBUGGING"):

        @contextmember
        @staticmethod
        def debug():
            """Enter a debugger at this line in the compiled jinja code."""
            import sys
            import ipdb  # type: ignore

            frame = sys._getframe(3)
            ipdb.set_trace(frame)
            return ""

    @contextmember("return")
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
    def tojson(value: Any, default: Any = None, sort_keys: bool = False) -> Any:
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

    @contextmember("set")
    @staticmethod
    def _set(value: Iterable[Any], default: Any = None) -> Optional[Set[Any]]:
        """The `set` context method can be used to convert any iterable
        to a sequence of iterable elements that are unique (a set).

        :param value: The iterable
        :param default: A default value to return if the `value` argument
            is not an iterable

        Usage:
            {% set my_list = [1, 2, 2, 3] %}
            {% set my_set = set(my_list) %}
            {% do log(my_set) %}  {# {1, 2, 3} #}
        """
        try:
            return set(value)
        except TypeError:
            return default

    @contextmember
    @staticmethod
    def try_set(value: Iterable[Any]) -> Set[Any]:
        """The `try_set` context method can be used to convert any iterable
        to a sequence of iterable elements that are unique (a set). The
        difference to the `set` context method is that the `try_set` method
        will raise an exception on a TypeError.

        :param value: The iterable
        :param default: A default value to return if the `value` argument
            is not an iterable

        Usage:
            {% set my_list = [1, 2, 2, 3] %}
            {% set my_set = try_set(my_list) %}
            {% do log(my_set) %}  {# {1, 2, 3} #}
        """
        try:
            return set(value)
        except TypeError as e:
            raise CompilationException(e)

    @contextmember("zip")
    @staticmethod
    def _zip(*args: Iterable[Any], default: Any = None) -> Optional[Iterable[Any]]:
        """The `try_zip` context method can be used to used to return
        an iterator of tuples, where the i-th tuple contains the i-th
        element from each of the argument iterables.

        :param *args: Any number of iterables
        :param default: A default value to return if `*args` is not
            iterable

        Usage:
            {% set my_list_a = [1, 2] %}
            {% set my_list_b = ['alice', 'bob'] %}
            {% set my_zip = zip(my_list_a, my_list_b) | list %}
            {% do log(my_set) %}  {# [(1, 'alice'), (2, 'bob')] #}
        """
        try:
            return zip(*args)
        except TypeError:
            return default

    @contextmember
    @staticmethod
    def try_zip(*args: Iterable[Any]) -> Iterable[Any]:
        """The `try_zip` context method can be used to used to return
        an iterator of tuples, where the i-th tuple contains the i-th
        element from each of the argument iterables. The difference to the
        `zip` context method is that the `try_zip` method will raise an
        exception on a TypeError.

        :param *args: Any number of iterables
        :param default: A default value to return if `*args` is not
            iterable

        Usage:
            {% set my_list_a = [1, 2] %}
            {% set my_list_b = ['alice', 'bob'] %}
            {% set my_zip = try_zip(my_list_a, my_list_b) | list %}
            {% do log(my_set) %}  {# [(1, 'alice'), (2, 'bob')] #}
        """
        try:
            return zip(*args)
        except TypeError as e:
            raise CompilationException(e)

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
            fire_event(MacroEventInfo(msg=msg))
        else:
            fire_event(MacroEventDebug(msg=msg))
        return ""

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
        return get_invocation_id()

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

        This supports all flags defined in flags submodule (core/dbt/flags.py)
        TODO: Replace with object that provides read-only access to flag values
        """
        return flags

    @contextmember
    @staticmethod
    def print(msg: str) -> str:
        """Prints a line to stdout.

        :param msg: The message to print

        > macros/my_log_macro.sql

            {% macro some_macro(arg1, arg2) %}
              {{ print("Running some_macro: " ~ arg1 ~ ", " ~ arg2) }}
            {% endmacro %}"
        """

        if not flags.NO_PRINT:
            print(msg)
        return ""


def generate_base_context(cli_vars: Dict[str, Any]) -> Dict[str, Any]:
    ctx = BaseContext(cli_vars)
    # This is not a Mashumaro to_dict call
    return ctx.to_dict()
