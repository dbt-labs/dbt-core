import abc
import os
from typing import (
    Callable, Any, Dict, Optional, Union, List, TypeVar, Type, Iterable,
    Mapping,
)
from typing_extensions import Protocol

from dbt.adapters.base.column import Column
from dbt.adapters.factory import (
    get_adapter, get_adapter_package_names, get_adapter_type_names
)
from dbt.clients import agate_helper
from dbt.clients.jinja import get_rendered, MacroGenerator, MacroStack, undefined_error
from dbt.config import RuntimeConfig, Project
from .base import contextmember, contextproperty, Var
from .configured import FQNLookup
from .context_config import ContextConfig
from dbt.context.macro_resolver import MacroResolver, TestMacroNamespace
from .macros import MacroNamespaceBuilder, MacroNamespace
from .manifest import ManifestContext
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import (
    Manifest, Disabled
)
from dbt.contracts.graph.compiled import (
    CompiledResource,
    CompiledSeedNode,
    ManifestNode,
)
from dbt.contracts.graph.parsed import (
    ParsedMacro,
    ParsedExposure,
    ParsedSeedNode,
    ParsedSourceDefinition,
)
from dbt.exceptions import (
    CompilationException,
    InternalException,
    ValidationException,
    RuntimeException,
    macro_invalid_dispatch_arg,
    missing_config,
    raise_compiler_error,
    ref_invalid_args,
    ref_target_not_found,
    ref_bad_context,
    source_target_not_found,
    wrapped_exports,
)
from dbt.config import IsFQNResource
from dbt.logger import GLOBAL_LOGGER as logger, SECRET_ENV_PREFIX  # noqa
from dbt.node_types import NodeType

from dbt.utils import (
    merge, AttrDict, MultiDict
)

import agate


_MISSING = object()


# base classes
class RelationProxy:
    def __init__(self, adapter):
        self._quoting_config = adapter.config.quoting
        self._relation_type = adapter.Relation

    def __getattr__(self, key):
        return getattr(self._relation_type, key)

    def create_from_source(self, *args, **kwargs):
        # bypass our create when creating from source so as not to mess up
        # the source quoting
        return self._relation_type.create_from_source(*args, **kwargs)

    def create(self, *args, **kwargs):
        kwargs['quote_policy'] = merge(
            self._quoting_config,
            kwargs.pop('quote_policy', {})
        )
        return self._relation_type.create(*args, **kwargs)


class BaseDatabaseWrapper:
    """
    Wrapper for runtime database interaction. Applies the runtime quote policy
    via a relation proxy.
    """

    def __init__(self, adapter, namespace: MacroNamespace):
        self._adapter = adapter
        self.Relation = RelationProxy(adapter)
        self._namespace = namespace

    def __getattr__(self, name):
        raise NotImplementedError('subclasses need to implement this')

    @property
    def config(self):
        return self._adapter.config

    def type(self):
        return self._adapter.type()

    def commit(self):
        return self._adapter.commit_if_has_connection()

    def _get_adapter_macro_prefixes(self) -> List[str]:
        # order matters for dispatch:
        #  1. current adapter
        #  2. any parent adapters (dependencies)
        #  3. 'default'
        search_prefixes = get_adapter_type_names(self._adapter.type()) + ['default']
        return search_prefixes

    def dispatch(
        self,
        macro_name: str,
        macro_namespace: Optional[str] = None,
        packages: Optional[List[str]] = None,  # eventually remove since it's fully deprecated
    ) -> MacroGenerator:
        search_packages: List[Optional[str]]

        if '.' in macro_name:
            suggest_macro_namespace, suggest_macro_name = macro_name.split('.', 1)
            msg = (
                f'In adapter.dispatch, got a macro name of "{macro_name}", '
                f'but "." is not a valid macro name component. Did you mean '
                f'`adapter.dispatch("{suggest_macro_name}", '
                f'macro_namespace="{suggest_macro_namespace}")`?'
            )
            raise CompilationException(msg)

        if packages is not None:
            raise macro_invalid_dispatch_arg(macro_name)

        namespace = macro_namespace

        if namespace is None:
            search_packages = [None]
        elif isinstance(namespace, str):
            search_packages = self._adapter.config.get_macro_search_order(namespace)
            if not search_packages and namespace in self._adapter.config.dependencies:
                search_packages = [self.config.project_name, namespace]
        else:
            # Not a string and not None so must be a list
            raise CompilationException(
                f'In adapter.dispatch, got a list macro_namespace argument '
                f'("{macro_namespace}"), but macro_namespace should be None or a string.'
            )

        attempts = []

        for package_name in search_packages:
            for prefix in self._get_adapter_macro_prefixes():
                search_name = f'{prefix}__{macro_name}'
                try:
                    # this uses the namespace from the context
                    macro = self._namespace.get_from_package(
                        package_name, search_name
                    )
                except CompilationException:
                    # Only raise CompilationException if macro is not found in
                    # any package
                    macro = None

                if package_name is None:
                    attempts.append(search_name)
                else:
                    attempts.append(f'{package_name}.{search_name}')

                if macro is not None:
                    return macro

        searched = ', '.join(repr(a) for a in attempts)
        msg = (
            f"In dispatch: No macro named '{macro_name}' found\n"
            f"    Searched for: {searched}"
        )
        raise CompilationException(msg)


class BaseResolver(metaclass=abc.ABCMeta):
    def __init__(self, db_wrapper, model, config, manifest):
        self.db_wrapper = db_wrapper
        self.model = model
        self.config = config
        self.manifest = manifest

    @property
    def current_project(self):
        return self.config.project_name

    @property
    def Relation(self):
        return self.db_wrapper.Relation

    @abc.abstractmethod
    def __call__(self, *args: str) -> Union[str, RelationProxy]:
        pass


class BaseRefResolver(BaseResolver):
    @abc.abstractmethod
    def resolve(
        self, name: str, package: Optional[str] = None
    ) -> RelationProxy:
        ...

    def _repack_args(
        self, name: str, package: Optional[str]
    ) -> List[str]:
        if package is None:
            return [name]
        else:
            return [package, name]

    def validate_args(self, name: str, package: Optional[str]):
        if not isinstance(name, str):
            raise CompilationException(
                f'The name argument to ref() must be a string, got '
                f'{type(name)}'
            )

        if package is not None and not isinstance(package, str):
            raise CompilationException(
                f'The package argument to ref() must be a string or None, got '
                f'{type(package)}'
            )

    def __call__(self, *args: str) -> RelationProxy:
        name: str
        package: Optional[str] = None

        if len(args) == 1:
            name = args[0]
        elif len(args) == 2:
            package, name = args
        else:
            ref_invalid_args(self.model, args)
        self.validate_args(name, package)
        return self.resolve(name, package)


class BaseSourceResolver(BaseResolver):
    @abc.abstractmethod
    def resolve(self, source_name: str, table_name: str):
        pass

    def validate_args(self, source_name: str, table_name: str):
        if not isinstance(source_name, str):
            raise CompilationException(
                f'The source name (first) argument to source() must be a '
                f'string, got {type(source_name)}'
            )
        if not isinstance(table_name, str):
            raise CompilationException(
                f'The table name (second) argument to source() must be a '
                f'string, got {type(table_name)}'
            )

    def __call__(self, *args: str) -> RelationProxy:
        if len(args) != 2:
            raise_compiler_error(
                f"source() takes exactly two arguments ({len(args)} given)",
                self.model
            )
        self.validate_args(args[0], args[1])
        return self.resolve(args[0], args[1])


class Config(Protocol):
    def __init__(self, model, context_config: Optional[ContextConfig]):
        ...


# Implementation of "config(..)" calls in models
class ParseConfigObject(Config):
    def __init__(self, model, context_config: Optional[ContextConfig]):
        self.model = model
        self.context_config = context_config

    def _transform_config(self, config):
        for oldkey in ('pre_hook', 'post_hook'):
            if oldkey in config:
                newkey = oldkey.replace('_', '-')
                if newkey in config:
                    raise_compiler_error(
                        'Invalid config, has conflicting keys "{}" and "{}"'
                        .format(oldkey, newkey),
                        self.model
                    )
                config[newkey] = config.pop(oldkey)
        return config

    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            raise_compiler_error(
                "Invalid inline model config",
                self.model)

        opts = self._transform_config(opts)

        # it's ok to have a parse context with no context config, but you must
        # not call it!
        if self.context_config is None:
            raise RuntimeException(
                'At parse time, did not receive a context config'
            )
        self.context_config.add_config_call(opts)
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def require(self, name, validator=None):
        return ''

    def get(self, name, validator=None, default=None):
        return ''

    def persist_relation_docs(self) -> bool:
        return False

    def persist_column_docs(self) -> bool:
        return False


class RuntimeConfigObject(Config):
    def __init__(
        self, model, context_config: Optional[ContextConfig] = None
    ):
        self.model = model
        # we never use or get a config, only the parser cares

    def __call__(self, *args, **kwargs):
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def _validate(self, validator, value):
        validator(value)

    def _lookup(self, name, default=_MISSING):
        # if this is a macro, there might be no `model.config`.
        if not hasattr(self.model, 'config'):
            result = default
        else:
            result = self.model.config.get(name, default)
        if result is _MISSING:
            missing_config(self.model, name)
        return result

    def require(self, name, validator=None):
        to_return = self._lookup(name)

        if validator is not None:
            self._validate(validator, to_return)

        return to_return

    def get(self, name, validator=None, default=None):
        to_return = self._lookup(name, default)

        if validator is not None and default is not None:
            self._validate(validator, to_return)

        return to_return

    def persist_relation_docs(self) -> bool:
        persist_docs = self.get('persist_docs', default={})
        if not isinstance(persist_docs, dict):
            raise_compiler_error(
                f"Invalid value provided for 'persist_docs'. Expected dict "
                f"but received {type(persist_docs)}")

        return persist_docs.get('relation', False)

    def persist_column_docs(self) -> bool:
        persist_docs = self.get('persist_docs', default={})
        if not isinstance(persist_docs, dict):
            raise_compiler_error(
                f"Invalid value provided for 'persist_docs'. Expected dict "
                f"but received {type(persist_docs)}")

        return persist_docs.get('columns', False)


# `adapter` implementations
class ParseDatabaseWrapper(BaseDatabaseWrapper):
    """The parser subclass of the database wrapper applies any explicit
    parse-time overrides.
    """

    def __getattr__(self, name):
        override = (name in self._adapter._available_ and
                    name in self._adapter._parse_replacements_)

        if override:
            return self._adapter._parse_replacements_[name]
        elif name in self._adapter._available_:
            return getattr(self._adapter, name)
        else:
            raise AttributeError(
                "'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, name
                )
            )


class RuntimeDatabaseWrapper(BaseDatabaseWrapper):
    """The runtime database wrapper exposes everything the adapter marks
    available.
    """

    def __getattr__(self, name):
        if name in self._adapter._available_:
            return getattr(self._adapter, name)
        else:
            raise AttributeError(
                "'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, name
                )
            )


# `ref` implementations
class ParseRefResolver(BaseRefResolver):
    def resolve(
        self, name: str, package: Optional[str] = None
    ) -> RelationProxy:
        self.model.refs.append(self._repack_args(name, package))

        return self.Relation.create_from(self.config, self.model)


ResolveRef = Union[Disabled, ManifestNode]


class RuntimeRefResolver(BaseRefResolver):
    def resolve(
        self, target_name: str, target_package: Optional[str] = None
    ) -> RelationProxy:
        target_model = self.manifest.resolve_ref(
            target_name,
            target_package,
            self.current_project,
            self.model.package_name,
        )

        if target_model is None or isinstance(target_model, Disabled):
            ref_target_not_found(
                self.model,
                target_name,
                target_package,
                disabled=isinstance(target_model, Disabled),
            )
        self.validate(target_model, target_name, target_package)
        return self.create_relation(target_model, target_name)

    def create_relation(
        self, target_model: ManifestNode, name: str
    ) -> RelationProxy:
        if target_model.is_ephemeral_model:
            self.model.set_cte(target_model.unique_id, None)
            return self.Relation.create_ephemeral_from_node(
                self.config, target_model
            )
        else:
            return self.Relation.create_from(self.config, target_model)

    def validate(
        self,
        resolved: ManifestNode,
        target_name: str,
        target_package: Optional[str]
    ) -> None:
        if resolved.unique_id not in self.model.depends_on.nodes:
            args = self._repack_args(target_name, target_package)
            ref_bad_context(self.model, args)


class OperationRefResolver(RuntimeRefResolver):
    def validate(
        self,
        resolved: ManifestNode,
        target_name: str,
        target_package: Optional[str],
    ) -> None:
        pass

    def create_relation(
        self, target_model: ManifestNode, name: str
    ) -> RelationProxy:
        if target_model.is_ephemeral_model:
            # In operations, we can't ref() ephemeral nodes, because
            # ParsedMacros do not support set_cte
            raise_compiler_error(
                'Operations can not ref() ephemeral nodes, but {} is ephemeral'
                .format(target_model.name),
                self.model
            )
        else:
            return super().create_relation(target_model, name)


# `source` implementations
class ParseSourceResolver(BaseSourceResolver):
    def resolve(self, source_name: str, table_name: str):
        # When you call source(), this is what happens at parse time
        self.model.sources.append([source_name, table_name])
        return self.Relation.create_from(self.config, self.model)


class RuntimeSourceResolver(BaseSourceResolver):
    def resolve(self, source_name: str, table_name: str):
        target_source = self.manifest.resolve_source(
            source_name,
            table_name,
            self.current_project,
            self.model.package_name,
        )

        if target_source is None or isinstance(target_source, Disabled):
            source_target_not_found(
                self.model,
                source_name,
                table_name,
            )
        return self.Relation.create_from_source(target_source)


# `var` implementations.
class ModelConfiguredVar(Var):
    def __init__(
        self,
        context: Dict[str, Any],
        config: RuntimeConfig,
        node: CompiledResource,
    ) -> None:
        self._node: CompiledResource
        self._config: RuntimeConfig = config
        super().__init__(context, config.cli_vars, node=node)

    def packages_for_node(self) -> Iterable[Project]:
        dependencies = self._config.load_dependencies()
        package_name = self._node.package_name

        if package_name != self._config.project_name:
            if package_name not in dependencies:
                # I don't think this is actually reachable
                raise_compiler_error(
                    f'Node package named {package_name} not found!',
                    self._node
                )
            yield dependencies[package_name]
        yield self._config

    def _generate_merged(self) -> Mapping[str, Any]:
        search_node: IsFQNResource
        if isinstance(self._node, IsFQNResource):
            search_node = self._node
        else:
            search_node = FQNLookup(self._node.package_name)

        adapter_type = self._config.credentials.type

        merged = MultiDict()
        for project in self.packages_for_node():
            merged.add(project.vars.vars_for(search_node, adapter_type))
        merged.add(self._cli_vars)
        return merged


class ParseVar(ModelConfiguredVar):
    def get_missing_var(self, var_name):
        # in the parser, just always return None.
        return None


class RuntimeVar(ModelConfiguredVar):
    pass


# Providers
class Provider(Protocol):
    execute: bool
    Config: Type[Config]
    DatabaseWrapper: Type[BaseDatabaseWrapper]
    Var: Type[ModelConfiguredVar]
    ref: Type[BaseRefResolver]
    source: Type[BaseSourceResolver]


class ParseProvider(Provider):
    execute = False
    Config = ParseConfigObject
    DatabaseWrapper = ParseDatabaseWrapper
    Var = ParseVar
    ref = ParseRefResolver
    source = ParseSourceResolver


class GenerateNameProvider(Provider):
    execute = False
    Config = RuntimeConfigObject
    DatabaseWrapper = ParseDatabaseWrapper
    Var = RuntimeVar
    ref = ParseRefResolver
    source = ParseSourceResolver


class RuntimeProvider(Provider):
    execute = True
    Config = RuntimeConfigObject
    DatabaseWrapper = RuntimeDatabaseWrapper
    Var = RuntimeVar
    ref = RuntimeRefResolver
    source = RuntimeSourceResolver


class OperationProvider(RuntimeProvider):
    ref = OperationRefResolver


T = TypeVar('T')


# Base context collection, used for parsing configs.
class ProviderContext(ManifestContext):
    # subclasses are MacroContext, ModelContext, TestContext
    def __init__(
        self,
        model,
        config: RuntimeConfig,
        manifest: Manifest,
        provider: Provider,
        context_config: Optional[ContextConfig],
    ) -> None:
        if provider is None:
            raise InternalException(
                f"Invalid provider given to context: {provider}"
            )
        # mypy appeasement - we know it'll be a RuntimeConfig
        self.config: RuntimeConfig
        self.model: Union[ParsedMacro, ManifestNode] = model
        super().__init__(config, manifest, model.package_name)
        self.sql_results: Dict[str, AttrDict] = {}
        self.context_config: Optional[ContextConfig] = context_config
        self.provider: Provider = provider
        self.adapter = get_adapter(self.config)
        # The macro namespace is used in creating the DatabaseWrapper
        self.db_wrapper = self.provider.DatabaseWrapper(
            self.adapter, self.namespace
        )

    # This overrides the method in ManifestContext, and provides
    # a model, which the ManifestContext builder does not
    def _get_namespace_builder(self):
        internal_packages = get_adapter_package_names(
            self.config.credentials.type
        )
        return MacroNamespaceBuilder(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
            internal_packages,
            self.model,
        )

    @contextproperty
    def _sql_results(self) -> Dict[str, AttrDict]:
        return self.sql_results

    @contextmember
    def load_result(self, name: str) -> Optional[AttrDict]:
        return self.sql_results.get(name)

    @contextmember
    def store_result(
        self, name: str,
        response: Any,
        agate_table: Optional[agate.Table] = None
    ) -> str:
        if agate_table is None:
            agate_table = agate_helper.empty_table()

        self.sql_results[name] = AttrDict({
            'response': response,
            'data': agate_helper.as_matrix(agate_table),
            'table': agate_table
        })
        return ''

    @contextmember
    def store_raw_result(
        self,
        name: str,
        message=Optional[str],
        code=Optional[str],
        rows_affected=Optional[str],
        agate_table: Optional[agate.Table] = None
    ) -> str:
        response = AdapterResponse(
            _message=message, code=code, rows_affected=rows_affected)
        return self.store_result(name, response, agate_table)

    @contextproperty
    def validation(self):
        def validate_any(*args) -> Callable[[T], None]:
            def inner(value: T) -> None:
                for arg in args:
                    if isinstance(arg, type) and isinstance(value, arg):
                        return
                    elif value == arg:
                        return
                raise ValidationException(
                    'Expected value "{}" to be one of {}'
                    .format(value, ','.join(map(str, args))))
            return inner

        return AttrDict({
            'any': validate_any,
        })

    @contextmember
    def write(self, payload: str) -> str:
        # macros/source defs aren't 'writeable'.
        if isinstance(self.model, (ParsedMacro, ParsedSourceDefinition)):
            raise_compiler_error(
                'cannot "write" macros or sources'
            )
        self.model.build_path = self.model.write_node(
            self.config.target_path, 'run', payload
        )
        return ''

    @contextmember
    def render(self, string: str) -> str:
        return get_rendered(string, self._ctx, self.model)

    @contextmember
    def try_or_compiler_error(
        self, message_if_exception: str, func: Callable, *args, **kwargs
    ) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            raise_compiler_error(
                message_if_exception, self.model
            )

    @contextmember
    def load_agate_table(self) -> agate.Table:
        if not isinstance(self.model, (ParsedSeedNode, CompiledSeedNode)):
            raise_compiler_error(
                'can only load_agate_table for seeds (got a {})'
                .format(self.model.resource_type)
            )
        path = os.path.join(
            self.model.root_path, self.model.original_file_path
        )
        column_types = self.model.config.column_types
        try:
            table = agate_helper.from_csv(path, text_columns=column_types)
        except ValueError as e:
            raise_compiler_error(str(e))
        table.original_abspath = os.path.abspath(path)
        return table

    @contextproperty
    def ref(self) -> Callable:
        """The most important function in dbt is `ref()`; it's impossible to
        build even moderately complex models without it. `ref()` is how you
        reference one model within another. This is a very common behavior, as
        typically models are built to be "stacked" on top of one another. Here
        is how this looks in practice:

        > model_a.sql:

            select *
            from public.raw_data

        > model_b.sql:

            select *
            from {{ref('model_a')}}


        `ref()` is, under the hood, actually doing two important things. First,
        it is interpolating the schema into your model file to allow you to
        change your deployment schema via configuration. Second, it is using
        these references between models to automatically build the dependency
        graph. This will enable dbt to deploy models in the correct order when
        using dbt run.

        The `ref` function returns a Relation object.

        ## Advanced ref usage

        There is also a two-argument variant of the `ref` function. With this
        variant, you can pass both a package name and model name to `ref` to
        avoid ambiguity. This functionality is not commonly required for
        typical dbt usage.

        > model.sql:

            select * from {{ ref('package_name', 'model_name') }}"
        """
        return self.provider.ref(
            self.db_wrapper, self.model, self.config, self.manifest
        )

    @contextproperty
    def source(self) -> Callable:
        return self.provider.source(
            self.db_wrapper, self.model, self.config, self.manifest
        )

    @contextproperty('config')
    def ctx_config(self) -> Config:
        """The `config` variable exists to handle end-user configuration for
        custom materializations. Configs like `unique_key` can be implemented
        using the `config` variable in your own materializations.

        For example, code in the `incremental` materialization like this:

            {% materialization incremental, default -%}
            {%- set unique_key = config.get('unique_key') -%}
            ...

        is responsible for handling model code that looks like this:

            {{
              config(
                materialized='incremental',
                unique_key='id'
              )
            }}


        ## config.get

        name: The name of the configuration variable (required)
        default: The default value to use if this configuration is not provided
            (optional)

        The `config.get` function is used to get configurations for a model
        from the end-user. Configs defined in this way are optional, and a
        default value can be provided.

        Example usage:

            {% materialization incremental, default -%}
              -- Example w/ no default. unique_key will be None if the user does not provide this configuration
              {%- set unique_key = config.get('unique_key') -%}
              -- Example w/ default value. Default to 'id' if 'unique_key' not provided
              {%- set unique_key = config.get('unique_key', default='id') -%}
              ...

        ## config.require

        name: The name of the configuration variable (required)

        The `config.require` function is used to get configurations for a model
        from the end-user. Configs defined using this function are required,
        and failure to provide them will result in a compilation error.

        Example usage:

            {% materialization incremental, default -%}
              {%- set unique_key = config.require('unique_key') -%}
              ...
        """  # noqa
        return self.provider.Config(self.model, self.context_config)

    @contextproperty
    def execute(self) -> bool:
        """`execute` is a Jinja variable that returns True when dbt is in
        "execute" mode.

        When you execute a dbt compile or dbt run command, dbt:

        - Reads all of the files in your project and generates a "manifest"
            comprised of models, tests, and other graph nodes present in your
            project. During this phase, dbt uses the `ref` statements it finds
            to generate the DAG for your project. *No SQL is run during this
            phase*, and `execute == False`.
        - Compiles (and runs) each node (eg. building models, or running
            tests). SQL is run during this phase, and `execute == True`.

        Any Jinja that relies on a result being returned from the database will
        error during the parse phase. For example, this SQL will return an
        error:

        > models/order_payment_methods.sql:

            {% set payment_method_query %}
            select distinct
            payment_method
            from {{ ref('raw_payments') }}
            order by 1
            {% endset %}
            {% set results = run_query(relation_query) %}
            {# Return the first column #}
            {% set payment_methods = results.columns[0].values() %}

        The error returned by dbt will look as follows:

            Encountered an error:
                Compilation Error in model order_payment_methods (models/order_payment_methods.sql)
            'None' has no attribute 'table'

        This is because Line #11 assumes that a table has been returned, when,
        during the parse phase, this query hasn't been run.

        To work around this, wrap any problematic Jinja in an
        `{% if execute %}` statement:

        > models/order_payment_methods.sql:

            {% set payment_method_query %}
            select distinct
            payment_method
            from {{ ref('raw_payments') }}
            order by 1
            {% endset %}
            {% set results = run_query(relation_query) %}
            {% if execute %}
            {# Return the first column #}
            {% set payment_methods = results.columns[0].values() %}
            {% else %}
            {% set payment_methods = [] %}
            {% endif %}
        """  # noqa
        return self.provider.execute

    @contextproperty
    def exceptions(self) -> Dict[str, Any]:
        """The exceptions namespace can be used to raise warnings and errors in
        dbt userspace.


        ## raise_compiler_error

        The `exceptions.raise_compiler_error` method will raise a compiler
        error with the provided message. This is typically only useful in
        macros or materializations when invalid arguments are provided by the
        calling model. Note that throwing an exception will cause a model to
        fail, so please use this variable with care!

        Example usage:

        > exceptions.sql:

            {% if number < 0 or number > 100 %}
              {{ exceptions.raise_compiler_error("Invalid `number`. Got: " ~ number) }}
            {% endif %}

        ## warn

        The `exceptions.warn` method will raise a compiler warning with the
        provided message. If the `--warn-error` flag is provided to dbt, then
        this warning will be elevated to an exception, which is raised.

        Example usage:

        > warn.sql:

            {% if number < 0 or number > 100 %}
              {% do exceptions.warn("Invalid `number`. Got: " ~ number) %}
            {% endif %}
        """  # noqa
        return wrapped_exports(self.model)

    @contextproperty
    def database(self) -> str:
        return self.config.credentials.database

    @contextproperty
    def schema(self) -> str:
        return self.config.credentials.schema

    @contextproperty
    def var(self) -> ModelConfiguredVar:
        return self.provider.Var(
            context=self._ctx,
            config=self.config,
            node=self.model,
        )

    @contextproperty('adapter')
    def ctx_adapter(self) -> BaseDatabaseWrapper:
        """`adapter` is a wrapper around the internal database adapter used by
        dbt. It allows users to make calls to the database in their dbt models.
        The adapter methods will be translated into specific SQL statements
        depending on the type of adapter your project is using.
        """
        return self.db_wrapper

    @contextproperty
    def api(self) -> Dict[str, Any]:
        return {
            'Relation': self.db_wrapper.Relation,
            'Column': self.adapter.Column,
        }

    @contextproperty
    def column(self) -> Type[Column]:
        return self.adapter.Column

    @contextproperty
    def env(self) -> Dict[str, Any]:
        return self.target

    @contextproperty
    def graph(self) -> Dict[str, Any]:
        """The `graph` context variable contains information about the nodes in
        your dbt project. Models, sources, tests, and snapshots are all
        examples of nodes in dbt projects.

        ## The graph context variable

        The graph context variable is a dictionary which maps node ids onto dictionary representations of those nodes. A simplified example might look like:

            {
              "model.project_name.model_name": {
                "config": {"materialzed": "table", "sort": "id"},
                "tags": ["abc", "123"],
                "path": "models/path/to/model_name.sql",
                ...
              },
              "source.project_name.source_name": {
                "path": "models/path/to/schema.yml",
                "columns": {
                  "id": { .... },
                  "first_name": { .... },
                },
                ...
              }
            }

        The exact contract for these model and source nodes is not currently
        documented, but that will change in the future.

        ## Accessing models

        The `model` entries in the `graph` dictionary will be incomplete or
        incorrect during parsing. If accessing the models in your project via
        the `graph` variable, be sure to use the `execute` flag to ensure that
        this code only executes at run-time and not at parse-time. Do not use
        the `graph` variable to build you DAG, as the resulting dbt behavior
        will be undefined and likely incorrect.

        Example usage:

        > graph-usage.sql:

            /*
              Print information about all of the models in the Snowplow package
            */
            {% if execute %}
              {% for node in graph.nodes.values()
                 | selectattr("resource_type", "equalto", "model")
                 | selectattr("package_name", "equalto", "snowplow") %}

                {% do log(node.unique_id ~ ", materialized: " ~ node.config.materialized, info=true) %}

              {% endfor %}
            {% endif %}
            /*
              Example output
            ---------------------------------------------------------------
            model.snowplow.snowplow_id_map, materialized: incremental
            model.snowplow.snowplow_page_views, materialized: incremental
            model.snowplow.snowplow_web_events, materialized: incremental
            model.snowplow.snowplow_web_page_context, materialized: table
            model.snowplow.snowplow_web_events_scroll_depth, materialized: incremental
            model.snowplow.snowplow_web_events_time, materialized: incremental
            model.snowplow.snowplow_web_events_internal_fixed, materialized: ephemeral
            model.snowplow.snowplow_base_web_page_context, materialized: ephemeral
            model.snowplow.snowplow_base_events, materialized: ephemeral
            model.snowplow.snowplow_sessions_tmp, materialized: incremental
            model.snowplow.snowplow_sessions, materialized: table
            */

        ## Accessing sources

        To access the sources in your dbt project programatically, use the "sources" attribute.

        Example usage:

        > models/events_unioned.sql

            /*
              Union all of the Snowplow sources defined in the project
              which begin with the string "event_"
            */
            {% set sources = [] -%}
            {% for node in graph.sources.values() -%}
              {%- if node.name.startswith('event_') and node.source_name == 'snowplow' -%}
                {%- do sources.append(source(node.source_name, node.name)) -%}
              {%- endif -%}
            {%- endfor %}
            select * from (
              {%- for source in sources %}
                {{ source }} {% if not loop.last %} union all {% endif %}
              {% endfor %}
            )
            /*
              Example compiled SQL
            ---------------------------------------------------------------
            select * from (
              select * from raw.snowplow.event_add_to_cart union all
              select * from raw.snowplow.event_remove_from_cart union all
              select * from raw.snowplow.event_checkout
            )
            */

        """  # noqa
        return self.manifest.flat_graph

    @contextproperty('model')
    def ctx_model(self) -> Dict[str, Any]:
        return self.model.to_dict(omit_none=True)

    @contextproperty
    def pre_hooks(self) -> Optional[List[Dict[str, Any]]]:
        return None

    @contextproperty
    def post_hooks(self) -> Optional[List[Dict[str, Any]]]:
        return None

    @contextproperty
    def sql(self) -> Optional[str]:
        return None

    @contextproperty
    def sql_now(self) -> str:
        return self.adapter.date_function()

    @contextmember
    def adapter_macro(self, name: str, *args, **kwargs):
        """This was deprecated in v0.18 in favor of adapter.dispatch
        """
        msg = (
            'The "adapter_macro" macro has been deprecated. Instead, use '
            'the `adapter.dispatch` method to find a macro and call the '
            'result.  For more information, see: '
            'https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch)'
            ' adapter_macro was called for: {macro_name}'
            .format(macro_name=name)
        )
        raise CompilationException(msg)

    @contextmember
    def env_var(self, var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.
        """
        return_value = None
        if var in os.environ:
            return_value = os.environ[var]
        elif default is not None:
            return_value = default

        if return_value is not None:
            # Save the env_var value in the manifest and the var name in the source_file
            if not var.startswith(SECRET_ENV_PREFIX) and self.model:
                self.manifest.env_vars[var] = return_value
                source_file = self.manifest.files[self.model.file_id]
                source_file.env_vars.append(var)
            return return_value
        else:
            msg = f"Env var required but not provided: '{var}'"
            undefined_error(msg)


class MacroContext(ProviderContext):
    """Internally, macros can be executed like nodes, with some restrictions:

     - they don't have have all values available that nodes do:
        - 'this', 'pre_hooks', 'post_hooks', and 'sql' are missing
        - 'schema' does not use any 'model' information
     - they can't be configured with config() directives
    """

    def __init__(
        self,
        model: ParsedMacro,
        config: RuntimeConfig,
        manifest: Manifest,
        provider: Provider,
        search_package: Optional[str],
    ) -> None:
        super().__init__(model, config, manifest, provider, None)
        # override the model-based package with the given one
        if search_package is None:
            # if the search package name isn't specified, use the root project
            self._search_package = config.project_name
        else:
            self._search_package = search_package


class ModelContext(ProviderContext):
    model: ManifestNode

    @contextproperty
    def pre_hooks(self) -> List[Dict[str, Any]]:
        if self.model.resource_type in [NodeType.Source, NodeType.Test]:
            return []
        return [
            h.to_dict(omit_none=True) for h in self.model.config.pre_hook
        ]

    @contextproperty
    def post_hooks(self) -> List[Dict[str, Any]]:
        if self.model.resource_type in [NodeType.Source, NodeType.Test]:
            return []
        return [
            h.to_dict(omit_none=True) for h in self.model.config.post_hook
        ]

    @contextproperty
    def sql(self) -> Optional[str]:
        if getattr(self.model, 'extra_ctes_injected', None):
            return self.model.compiled_sql
        return None

    @contextproperty
    def database(self) -> str:
        return getattr(
            self.model, 'database', self.config.credentials.database
        )

    @contextproperty
    def schema(self) -> str:
        return getattr(
            self.model, 'schema', self.config.credentials.schema
        )

    @contextproperty
    def this(self) -> Optional[RelationProxy]:
        """`this` makes available schema information about the currently
        executing model. It's is useful in any context in which you need to
        write code that references the current model, for example when defining
        a `sql_where` clause for an incremental model and for writing pre- and
        post-model hooks that operate on the model in some way. Developers have
        options for how to use `this`:

            |------------------|------------------|
            | dbt Model Syntax | Output           |
            |------------------|------------------|
            |     {{this}}     | "schema"."table" |
            |------------------|------------------|
            |  {{this.schema}} | schema           |
            |------------------|------------------|
            |  {{this.table}}  | table            |
            |------------------|------------------|
            |  {{this.name}}   | table            |
            |------------------|------------------|

        Here's an example of how to use `this` in `dbt_project.yml` to grant
        select rights on a table to a different db user.

        > example.yml:

            models:
              project-name:
                post-hook:
                  - "grant select on {{ this }} to db_reader"
        """
        if self.model.resource_type == NodeType.Operation:
            return None
        return self.db_wrapper.Relation.create_from(self.config, self.model)


# This is called by '_context_for', used in 'render_with_context'
def generate_parser_model_context(
    model: ManifestNode,
    config: RuntimeConfig,
    manifest: Manifest,
    context_config: ContextConfig,
) -> Dict[str, Any]:
    # The __init__ method of ModelContext also initializes
    # a ManifestContext object which creates a MacroNamespaceBuilder
    # which adds every macro in the Manifest.
    ctx = ModelContext(
        model, config, manifest, ParseProvider(), context_config
    )
    # The 'to_dict' method in ManifestContext moves all of the macro names
    # in the macro 'namespace' up to top level keys
    return ctx.to_dict()


def generate_generate_name_macro_context(
    macro: ParsedMacro,
    config: RuntimeConfig,
    manifest: Manifest,
) -> Dict[str, Any]:
    ctx = MacroContext(
        macro, config, manifest, GenerateNameProvider(), None
    )
    return ctx.to_dict()


def generate_runtime_model_context(
    model: ManifestNode,
    config: RuntimeConfig,
    manifest: Manifest,
) -> Dict[str, Any]:
    ctx = ModelContext(
        model, config, manifest, RuntimeProvider(), None
    )
    return ctx.to_dict()


def generate_runtime_macro_context(
    macro: ParsedMacro,
    config: RuntimeConfig,
    manifest: Manifest,
    package_name: Optional[str],
) -> Dict[str, Any]:
    ctx = MacroContext(
        macro, config, manifest, OperationProvider(), package_name
    )
    return ctx.to_dict()


class ExposureRefResolver(BaseResolver):
    def __call__(self, *args) -> str:
        if len(args) not in (1, 2):
            ref_invalid_args(self.model, args)
        self.model.refs.append(list(args))
        return ''


class ExposureSourceResolver(BaseResolver):
    def __call__(self, *args) -> str:
        if len(args) != 2:
            raise_compiler_error(
                f"source() takes exactly two arguments ({len(args)} given)",
                self.model
            )
        self.model.sources.append(list(args))
        return ''


def generate_parse_exposure(
    exposure: ParsedExposure,
    config: RuntimeConfig,
    manifest: Manifest,
    package_name: str,
) -> Dict[str, Any]:
    project = config.load_dependencies()[package_name]
    return {
        'ref': ExposureRefResolver(
            None,
            exposure,
            project,
            manifest,
        ),
        'source': ExposureSourceResolver(
            None,
            exposure,
            project,
            manifest,
        )
    }


# This class is currently used by the schema parser in order
# to limit the number of macros in the context by using
# the TestMacroNamespace
class TestContext(ProviderContext):
    def __init__(
        self,
        model,
        config: RuntimeConfig,
        manifest: Manifest,
        provider: Provider,
        context_config: Optional[ContextConfig],
        macro_resolver: MacroResolver,
    ) -> None:
        # this must be before super init so that macro_resolver exists for
        # build_namespace
        self.macro_resolver = macro_resolver
        self.thread_ctx = MacroStack()
        super().__init__(model, config, manifest, provider, context_config)
        self._build_test_namespace()
        # We need to rebuild this because it's already been built by
        # the ProviderContext with the wrong namespace.
        self.db_wrapper = self.provider.DatabaseWrapper(
            self.adapter, self.namespace
        )

    def _build_namespace(self):
        return {}

    # this overrides _build_namespace in ManifestContext which provides a
    # complete namespace of all macros to only specify macros in the depends_on
    # This only provides a namespace with macros in the test node
    # 'depends_on.macros' by using the TestMacroNamespace
    def _build_test_namespace(self):
        depends_on_macros = []
        if self.model.depends_on and self.model.depends_on.macros:
            depends_on_macros = self.model.depends_on.macros
        lookup_macros = depends_on_macros.copy()
        for macro_unique_id in lookup_macros:
            lookup_macro = self.macro_resolver.macros.get(macro_unique_id)
            if lookup_macro:
                depends_on_macros.extend(lookup_macro.depends_on.macros)

        macro_namespace = TestMacroNamespace(
            self.macro_resolver, self._ctx, self.model, self.thread_ctx,
            depends_on_macros
        )
        self.namespace = macro_namespace


def generate_test_context(
    model: ManifestNode,
    config: RuntimeConfig,
    manifest: Manifest,
    context_config: ContextConfig,
    macro_resolver: MacroResolver
) -> Dict[str, Any]:
    ctx = TestContext(
        model, config, manifest, ParseProvider(), context_config,
        macro_resolver
    )
    # The 'to_dict' method in ManifestContext moves all of the macro names
    # in the macro 'namespace' up to top level keys
    return ctx.to_dict()
