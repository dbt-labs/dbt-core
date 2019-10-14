from dbt.utils import get_materialization, add_ephemeral_model_prefix

import dbt.clients.jinja
import dbt.context.base
import dbt.context.common
import dbt.flags
from dbt.parser.util import ParserUtils

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class RefResolver(dbt.context.common.BaseResolver):
    def resolve(self, args):
        name = None
        package = None

        if len(args) == 1:
            name = args[0]
        elif len(args) == 2:
            package, name = args
        else:
            dbt.exceptions.ref_invalid_args(self.model, args)

        target_model = ParserUtils.resolve_ref(
            self.manifest,
            name,
            package,
            self.current_project,
            self.model.package_name)

        if target_model is None or target_model is ParserUtils.DISABLED:
            dbt.exceptions.ref_target_not_found(
                self.model,
                name,
                package)
        return target_model, name

    def create_ephemeral_relation(self, target_model, name):
        self.model.set_cte(target_model.unique_id, None)
        return self.Relation.create(
            type=self.Relation.CTE,
            identifier=add_ephemeral_model_prefix(name)
        ).quote(identifier=False)

    def create_relation(self, target_model, name):
        if get_materialization(target_model) == 'ephemeral':
            return self.create_ephemeral_relation(target_model, name)
        else:
            return self.Relation.create_from(self.config, target_model)

    def validate(self, resolved, args):
        if resolved.unique_id not in self.model.depends_on.nodes:
            dbt.exceptions.ref_bad_context(self.model, args)

    def __call__(self, *args):
        # When you call ref(), this is what happens at runtime
        target_model, name = self.resolve(args)
        self.validate(target_model, args)
        return self.create_relation(target_model, name)


class SourceResolver(dbt.context.common.BaseResolver):
    def resolve(self, source_name, table_name):
        target_source = ParserUtils.resolve_source(
            self.manifest,
            source_name,
            table_name,
            self.current_project,
            self.model.package_name
        )

        if target_source is None:
            dbt.exceptions.source_target_not_found(
                self.model,
                source_name,
                table_name)
        return target_source

    def __call__(self, source_name, table_name):
        """When you call source(), this is what happens at runtime"""
        target_source = self.resolve(source_name, table_name)
        return self.Relation.create_from_source(target_source)


_MISSING = object()


class Config:
    def __init__(self, model, source_config=None):
        self.model = model
        # we never use or get a source config, only the parser cares

    def __call__(*args, **kwargs):
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
            dbt.exceptions.missing_config(self.model, name)

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


class DatabaseWrapper(dbt.context.common.BaseDatabaseWrapper):
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


class Var(dbt.context.base.Var):
    pass


class Provider(dbt.context.common.Provider):
    execute = True
    Config = Config
    DatabaseWrapper = DatabaseWrapper
    Var = Var
    ref = RefResolver
    source = SourceResolver


def generate(model, runtime_config, manifest):
    return dbt.context.common.generate(
        model, runtime_config, manifest, None, Provider())
