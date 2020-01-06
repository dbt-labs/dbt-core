import agate
import os
from typing_extensions import Protocol
from typing import Union, Callable, Any, Dict, TypeVar, Type

from dbt.clients import agate_helper
from dbt.contracts.graph.compiled import CompiledSeedNode
from dbt.contracts.graph.parsed import ParsedSeedNode
import dbt.exceptions
import dbt.flags
import dbt.tracking
import dbt.utils
import dbt.writer
from dbt.adapters.factory import get_adapter
from dbt.node_types import NodeType
from dbt.clients.jinja import get_rendered
from dbt.context.base import Var, HasCredentialsContext
from dbt.contracts.graph.manifest import Manifest


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
        kwargs['quote_policy'] = dbt.utils.merge(
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


class BaseResolver:
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


class Config(Protocol):
    def __init__(self, model, source_config):
        ...


class Provider(Protocol):
    execute: bool
    Config: Type[Config]
    DatabaseWrapper: Type[BaseDatabaseWrapper]
    Var: Type[Var]
    ref: Type[BaseResolver]
    source: Type[BaseResolver]


class ManifestParsedContext(HasCredentialsContext):
    """A context available after the manifest has been parsed."""
    def __init__(self, config, manifest):
        super().__init__(config)
        self.manifest = manifest

    def add_macros(self, context):
        self.add_macros_from(context, self.manifest.macros)


def _store_result(sql_results):
    def call(name, status, agate_table=None):
        if agate_table is None:
            agate_table = agate_helper.empty_table()

        sql_results[name] = dbt.utils.AttrDict({
            'status': status,
            'data': agate_helper.as_matrix(agate_table),
            'table': agate_table
        })
        return ''

    return call


def _load_result(sql_results):
    def call(name):
        return sql_results.get(name)

    return call


T = TypeVar('T')


def get_validation() -> dbt.utils.AttrDict:
    def validate_any(*args) -> Callable[[T], None]:
        def inner(value: T) -> None:
            for arg in args:
                if isinstance(arg, type) and isinstance(value, arg):
                    return
                elif value == arg:
                    return
            raise dbt.exceptions.ValidationException(
                'Expected value "{}" to be one of {}'
                .format(value, ','.join(map(str, args))))
        return inner

    return dbt.utils.AttrDict({
        'any': validate_any,
    })


def add_sql_handlers(context):
    sql_results = {}
    context['_sql_results'] = sql_results
    context['store_result'] = _store_result(sql_results)
    context['load_result'] = _load_result(sql_results)


def write(node, target_path, subdirectory):
    def fn(payload):
        node.build_path = dbt.writer.write_node(
            node, target_path, subdirectory, payload)
        return ''

    return fn


def render(context, node):
    def fn(string):
        return get_rendered(string, context, node)

    return fn


def try_or_compiler_error(model):
    def impl(message_if_exception, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            dbt.exceptions.raise_compiler_error(message_if_exception, model)
    return impl


# Base context collection, used for parsing configs.


def _build_load_agate_table(
    model: Union[ParsedSeedNode, CompiledSeedNode]
) -> Callable[[], agate.Table]:
    def load_agate_table():
        path = model.seed_file_path
        column_types = model.config.column_types
        try:
            table = agate_helper.from_csv(path, text_columns=column_types)
        except ValueError as e:
            dbt.exceptions.raise_compiler_error(str(e))
        table.original_abspath = os.path.abspath(path)
        return table
    return load_agate_table


class ProviderContext(ManifestParsedContext):
    def __init__(self, model, config, manifest, provider, source_config):
        if provider is None:
            raise dbt.exceptions.InternalException(
                "Invalid provider given to context: {}".format(provider))
        self.model = model
        super().__init__(config, manifest)
        self.source_config = source_config
        self.provider = provider
        self.adapter = get_adapter(self.config)
        self.db_wrapper = self.provider.DatabaseWrapper(self.adapter)

    @property
    def search_package_name(self):
        return self.model.package_name

    def add_provider_functions(self, context):
        # Generate the builtin functions
        builtins = {
            'ref': self.provider.ref(
                self.db_wrapper, self.model, self.config, self.manifest),
            'source': self.provider.source(
                self.db_wrapper, self.model, self.config, self.manifest),
            'config': self.provider.Config(
                self.model, self.source_config),
            'execute': self.provider.execute
        }
        # Install them at .builtins
        context['builtins'] = builtins
        # Install each of them directly in case they're not
        # clobbered by a macro.
        context.update(builtins)

    def add_exceptions(self, context):
        context['exceptions'] = dbt.exceptions.wrapped_exports(self.model)

    def add_default_schema_info(self, context):
        context['database'] = getattr(
            self.model, 'database', self.config.credentials.database
        )
        context['schema'] = getattr(
            self.model, 'schema', self.config.credentials.schema
        )

    def make_var(self, context) -> Var:
        return self.provider.Var(
            self.model, context=context, overrides=self.config.cli_vars
        )

    def insert_model_information(self, context: Dict[str, Any]) -> None:
        """By default, the model information is not added to the context"""
        pass

    def modify_generated_context(self, context: Dict[str, Any]) -> None:
        context['validation'] = get_validation()
        add_sql_handlers(context)
        self.add_macros(context)

        context["write"] = write(self.model, self.config.target_path, 'run')
        context["render"] = render(context, self.model)
        context['context'] = context

    def to_dict(self):
        target = self.get_target()

        context = super().to_dict()

        self.add_provider_functions(context)
        self.add_exceptions(context)
        self.add_default_schema_info(context)

        context.update({
            "adapter": self.db_wrapper,
            "api": {
                "Relation": self.db_wrapper.Relation,
                "Column": self.adapter.Column,
            },
            "column": self.adapter.Column,
            'env': target,
            'target': target,
            "flags": dbt.flags,
            "load_agate_table": _build_load_agate_table(self.model),
            "graph": self.manifest.flat_graph,
            "model": self.model.to_dict(),
            "post_hooks": None,
            "pre_hooks": None,
            "sql": None,
            "sql_now": self.adapter.date_function(),
            "try_or_compiler_error": try_or_compiler_error(self.model)
        })

        self.insert_model_information(context)

        self.modify_generated_context(context)

        return context


class ExecuteMacroContext(ProviderContext):
    """Internally, macros can be executed like nodes, with some restrictions:

     - they don't have have all values available that nodes do:
        - 'this', 'pre_hooks', 'post_hooks', and 'sql' are missing
        - 'schema' does not use any 'model' information
     - they can't be configured with config() directives
    """
    def __init__(self, model, config, manifest: Manifest, provider) -> None:
        super().__init__(model, config, manifest, provider, None)


class ModelContext(ProviderContext):
    def get_this(self):
        return self.db_wrapper.Relation.create_from(self.config, self.model)

    def add_hooks(self, context):
        context['pre_hooks'] = [
            h.to_dict() for h in self.model.config.pre_hook
        ]
        context['post_hooks'] = [
            h.to_dict() for h in self.model.config.post_hook
        ]

    def insert_model_information(self, context):
        # operations (hooks) don't get a 'this'
        if self.model.resource_type != NodeType.Operation:
            context['this'] = self.get_this()
        # overwrite schema/database if we have them, and hooks + sql
        # the hooks should come in as dicts, at least for the `run_hooks` macro
        # TODO: do we have to preserve this as backwards a compatibility thing?
        self.add_default_schema_info(context)
        self.add_hooks(context)
        context['sql'] = getattr(self.model, 'injected_sql', None)


def generate_execute_macro(
    model, config, manifest: Manifest, provider
) -> Dict[str, Any]:
    """Internally, macros can be executed like nodes, with some restrictions:

     - they don't have have all values available that nodes do:
        - 'this', 'pre_hooks', 'post_hooks', and 'sql' are missing
        - 'schema' does not use any 'model' information
     - they can't be configured with config() directives
    """
    ctx = ExecuteMacroContext(model, config, manifest, provider)
    return ctx.to_dict()


def generate(
    model, config, manifest: Manifest, provider, source_config=None
) -> Dict[str, Any]:
    """
    Not meant to be called directly. Call with either:
        dbt.context.parser.generate
    or
        dbt.context.runtime.generate
    """
    ctx = ModelContext(model, config, manifest, provider, source_config)
    return ctx.to_dict()
