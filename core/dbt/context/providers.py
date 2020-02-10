import abc
import os
from typing import (
    Callable, Any, Dict, Optional, Union, List, TypeVar, Type
)
from typing_extensions import Protocol


from dbt.adapters.base.column import Column
from dbt.adapters.factory import get_adapter
from dbt.clients import agate_helper
from dbt.clients.jinja import get_rendered
from dbt.config import RuntimeConfig
from dbt.context.base import (
    contextmember, contextproperty, Var
)
from dbt.context.configured import ManifestContext, MacroNamespace
from dbt.contracts.graph.manifest import Manifest, Disabled
from dbt.contracts.graph.compiled import (
    NonSourceNode, CompiledSeedNode
)
from dbt.contracts.graph.parsed import (
    ParsedMacro, ParsedSourceDefinition, ParsedSeedNode
)
from dbt.exceptions import (
    InternalException,
    ValidationException,
    missing_config,
    raise_compiler_error,
    ref_invalid_args,
    ref_target_not_found,
    ref_bad_context,
    source_target_not_found,
    wrapped_exports,
)
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_types import NodeType
from dbt.source_config import SourceConfig

from dbt.utils import (
    add_ephemeral_model_prefix, merge, AttrDict
)

import agate


_MISSING = object()


# base classes
class RelationProxy:
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
        kwargs['quote_policy'] = merge(
            self.quoting_config,
            kwargs.pop('quote_policy', {})
        )
        return self.relation_type.create(*args, **kwargs)


class BaseDatabaseWrapper:
    """
    Wrapper for runtime database interaction. Applies the runtime quote policy
    via a relation proxy.
    """
    def __init__(self, adapter):
        self.adapter = adapter
        self.Relation = RelationProxy(adapter)

    def __getattr__(self, name):
        raise NotImplementedError('subclasses need to implement this')

    @property
    def config(self):
        return self.adapter.config

    def type(self):
        return self.adapter.type()

    def commit(self):
        return self.adapter.commit_if_has_connection()


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

    def __call__(self, *args: str) -> RelationProxy:
        name: str
        package: Optional[str] = None

        if len(args) == 1:
            name = args[0]
        elif len(args) == 2:
            package, name = args
        else:
            ref_invalid_args(self.model, args)
        return self.resolve(name, package)


class BaseSourceResolver(BaseResolver):
    @abc.abstractmethod
    def resolve(self, source_name: str, table_name: str):
        pass

    def __call__(self, *args: str) -> RelationProxy:
        if len(args) != 2:
            raise_compiler_error(
                f"source() takes exactly two arguments ({len(args)} given)",
                self.model
            )
        return self.resolve(args[0], args[1])


class Config(Protocol):
    def __init__(self, model, source_config):
        ...


class Provider(Protocol):
    execute: bool
    Config: Type[Config]
    DatabaseWrapper: Type[BaseDatabaseWrapper]
    Var: Type[Var]
    ref: Type[BaseRefResolver]
    source: Type[BaseSourceResolver]


# `config` implementations
class ParseConfigObject(Config):
    def __init__(self, model, source_config):
        self.model = model
        self.source_config = source_config

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

        self.source_config.update_in_model_config(opts)
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def require(self, name, validator=None):
        return ''

    def get(self, name, validator=None, default=None):
        return ''


class RuntimeConfigObject(Config):
    def __init__(self, model, source_config=None):
        self.model = model
        # we never use or get a source config, only the parser cares

    def __call__(self, *args, **kwargs):
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def _validate(self, validator, value):
        validator(value)

    def _lookup(self, name, default=_MISSING):
        config = self.model.config

        if hasattr(config, name):
            return getattr(config, name)
        elif name in config.extra:
            return config.extra[name]
        elif default is not _MISSING:
            return default
        else:
            missing_config(self.model, name)

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


# `adapter` implementations
class ParseDatabaseWrapper(BaseDatabaseWrapper):
    """The parser subclass of the database wrapper applies any explicit
    parse-time overrides.
    """
    def __getattr__(self, name):
        override = (name in self.adapter._available_ and
                    name in self.adapter._parse_replacements_)

        if override:
            return self.adapter._parse_replacements_[name]
        elif name in self.adapter._available_:
            return getattr(self.adapter, name)
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
        if name in self.adapter._available_:
            return getattr(self.adapter, name)
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


ResolveRef = Union[Disabled, NonSourceNode]


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
            )
        self.validate(target_model, target_name, target_package)
        return self.create_relation(target_model, target_name)

    def create_ephemeral_relation(
        self, target_model: NonSourceNode, name: str
    ) -> RelationProxy:
        self.model.set_cte(target_model.unique_id, None)
        return self.Relation.create(
            type=self.Relation.CTE,
            identifier=add_ephemeral_model_prefix(name)
        ).quote(identifier=False)

    def create_relation(
        self, target_model: NonSourceNode, name: str
    ) -> RelationProxy:
        if target_model.get_materialization() == 'ephemeral':
            return self.create_ephemeral_relation(target_model, name)
        else:
            return self.Relation.create_from(self.config, target_model)

    def validate(
        self,
        resolved: NonSourceNode,
        target_name: str,
        target_package: Optional[str]
    ) -> None:
        if resolved.unique_id not in self.model.depends_on.nodes:
            args = self._repack_args(target_name, target_package)
            ref_bad_context(self.model, args)


class OperationRefResolver(RuntimeRefResolver):
    def validate(
        self,
        resolved: NonSourceNode,
        target_name: str,
        target_package: Optional[str],
    ) -> None:
        pass

    def create_ephemeral_relation(
        self, target_model: NonSourceNode, name: str
    ) -> RelationProxy:
        # In operations, we can't ref() ephemeral nodes, because ParsedMacros
        # do not support set_cte
        raise_compiler_error(
            'Operations can not ref() ephemeral nodes, but {} is ephemeral'
            .format(target_model.name),
            self.model
        )


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

        if target_source is None:
            source_target_not_found(
                self.model,
                source_name,
                table_name,
            )
        return self.Relation.create_from_source(target_source)


# `var` implementations.
class ParseVar(Var):
    def get_missing_var(self, var_name):
        # in the parser, just always return None.
        return None


class RuntimeVar(Var):
    pass


# Providers
class ParseProvider(Provider):
    execute = False
    Config = ParseConfigObject
    DatabaseWrapper = ParseDatabaseWrapper
    Var = ParseVar
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
    def __init__(self, model, config, manifest, provider, source_config):
        if provider is None:
            raise InternalException(
                f"Invalid provider given to context: {provider}"
            )
        super().__init__(config, manifest, model.package_name)
        self.sql_results: Dict[str, AttrDict] = {}
        self.model: Union[ParsedMacro, NonSourceNode] = model
        self.source_config = source_config
        self.provider: Provider = provider
        self.adapter = get_adapter(self.config)
        self.db_wrapper = self.provider.DatabaseWrapper(self.adapter)

    def _get_namespace(self):
        return MacroNamespace(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
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
        self, name: str, status: Any, agate_table: Optional[agate.Table] = None
    ) -> str:
        if agate_table is None:
            agate_table = agate_helper.empty_table()

        self.sql_results[name] = AttrDict({
            'status': status,
            'data': agate_helper.as_matrix(agate_table),
            'table': agate_table
        })
        return ''

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
        path = self.model.seed_file_path
        column_types = self.model.config.column_types
        try:
            table = agate_helper.from_csv(path, text_columns=column_types)
        except ValueError as e:
            raise_compiler_error(str(e))
        table.original_abspath = os.path.abspath(path)
        return table

    @contextproperty
    def ref(self) -> Callable:
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
        return self.provider.Config(self.model, self.source_config)

    @contextproperty
    def execute(self) -> bool:
        return self.provider.execute

    @contextproperty
    def exceptions(self) -> Dict[str, Any]:
        return wrapped_exports(self.model)

    @contextproperty
    def database(self) -> str:
        return self.config.credentials.database

    @contextproperty
    def schema(self) -> str:
        return self.config.credentials.schema

    @contextproperty
    def var(self) -> Var:
        return self.provider.Var(
            self.model, context=self._ctx, overrides=self.config.cli_vars
        )

    @contextproperty('adapter')
    def ctx_adapter(self) -> BaseDatabaseWrapper:
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
        return self.manifest.flat_graph

    @contextproperty('model')
    def ctx_model(self) -> Dict[str, Any]:
        return self.model.to_dict()

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
    model: NonSourceNode

    @contextproperty
    def pre_hooks(self) -> List[Dict[str, Any]]:
        if isinstance(self.model, ParsedSourceDefinition):
            return []
        return [
            h.to_dict() for h in self.model.config.pre_hook
        ]

    @contextproperty
    def post_hooks(self) -> List[Dict[str, Any]]:
        if isinstance(self.model, ParsedSourceDefinition):
            return []
        return [
            h.to_dict() for h in self.model.config.post_hook
        ]

    @contextproperty
    def sql(self) -> Optional[str]:
        return getattr(self.model, 'injected_sql', None)

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
        if self.model.resource_type == NodeType.Operation:
            return None
        return self.db_wrapper.Relation.create_from(self.config, self.model)


def generate_parser_model(
    model: NonSourceNode,
    config: RuntimeConfig,
    manifest: Manifest,
    source_config: SourceConfig,
) -> Dict[str, Any]:
    ctx = ModelContext(
        model, config, manifest, ParseProvider(), source_config
    )
    return ctx.to_dict()


def generate_parser_macro(
    macro: ParsedMacro,
    config: RuntimeConfig,
    manifest: Manifest,
    package_name: Optional[str],
) -> Dict[str, Any]:
    ctx = MacroContext(
        macro, config, manifest, ParseProvider(), package_name
    )
    return ctx.to_dict()


def generate_runtime_model(
    model: NonSourceNode,
    config: RuntimeConfig,
    manifest: Manifest,
) -> Dict[str, Any]:
    ctx = ModelContext(
        model, config, manifest, RuntimeProvider(), None
    )
    return ctx.to_dict()


def generate_runtime_macro(
    macro: ParsedMacro,
    config: RuntimeConfig,
    manifest: Manifest,
    package_name: Optional[str],
) -> Dict[str, Any]:
    ctx = MacroContext(
        macro, config, manifest, OperationProvider(), package_name
    )
    return ctx.to_dict()
