import dbt.exceptions

import dbt.context.common
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import Docref


def docs(unparsed, docrefs, column_name=None):

    def do_docs(*args):
        if len(args) != 1 and len(args) != 2:
            dbt.exceptions.doc_invalid_args(unparsed, args)
        doc_package_name = ''
        doc_name = args[0]
        if len(args) == 2:
            doc_package_name = args[1]

        docref = Docref(documentation_package=doc_package_name,
                        documentation_name=doc_name,
                        column_name=column_name)
        docrefs.append(docref)

        # At parse time, nothing should care about what doc() returns
        return ''

    return do_docs


class Config:
    def __init__(self, model, source_config):
        self.model = model
        self.source_config = source_config

    def _transform_config(self, config):
        for oldkey in ('pre_hook', 'post_hook'):
            if oldkey in config:
                newkey = oldkey.replace('_', '-')
                if newkey in config:
                    dbt.exceptions.raise_compiler_error(
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
            dbt.exceptions.raise_compiler_error(
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


class DatabaseWrapper(dbt.context.common.BaseDatabaseWrapper):
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


class Var(dbt.context.base.Var):
    def get_missing_var(self, var_name):
        # in the parser, just always return None.
        return None


class RefResolver(dbt.context.common.BaseResolver):
    def __call__(self, *args):
        # When you call ref(), this is what happens at parse time
        if len(args) == 1 or len(args) == 2:
            self.model.refs.append(list(args))

        else:
            dbt.exceptions.ref_invalid_args(self.model, args)

        return self.Relation.create_from(self.config, self.model)


class SourceResolver(dbt.context.common.BaseResolver):
    def __call__(self, *args):
        # When you call source(), this is what happens at parse time
        if len(args) == 2:
            self.model.sources.append(list(args))

        else:
            dbt.exceptions.raise_compiler_error(
                "source() takes exactly two arguments ({} given)"
                .format(len(args)), self.model)

        return self.Relation.create_from(self.config, self.model)


class Provider(dbt.context.common.Provider):
    execute = False
    Config = Config
    DatabaseWrapper = DatabaseWrapper
    Var = Var
    ref = RefResolver
    source = SourceResolver


def generate(model, runtime_config, manifest, source_config):
    # during parsing, we don't have a connection, but we might need one, so we
    # have to acquire it.
    # In the future, it would be nice to lazily open the connection, as in some
    # projects it would be possible to parse without connecting to the db
    with get_adapter(runtime_config).connection_named(model.name):
        return dbt.context.common.generate(
            model, runtime_config, manifest, source_config, Provider()
        )


def generate_macro(model, runtime_config, manifest):
    # parser.generate_macro is called by the get_${attr}_func family of Parser
    # methods, which preparse and cache the generate_${attr}_name family of
    # macros for use during parsing
    return dbt.context.common.generate_execute_macro(
        model, runtime_config, manifest, Provider()
    )
