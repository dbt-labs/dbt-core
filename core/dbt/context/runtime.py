from dbt.utils import get_materialization, add_ephemeral_model_prefix

import dbt.clients.jinja
import dbt.context.common
import dbt.flags
from dbt.parser import ParserUtils

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


execute = True


def ref(db_wrapper, model, config, manifest):
    current_project = config.project_name
    adapter = db_wrapper.adapter

    def do_ref(*args):
        target_model_name = None
        target_model_package = None

        if len(args) == 1:
            target_model_name = args[0]
        elif len(args) == 2:
            target_model_package, target_model_name = args
        else:
            dbt.exceptions.ref_invalid_args(model, args)

        target_model = ParserUtils.resolve_ref(
            manifest,
            target_model_name,
            target_model_package,
            current_project,
            model.get('package_name'))

        if target_model is None or target_model is ParserUtils.DISABLED:
            dbt.exceptions.ref_target_not_found(
                model,
                target_model_name,
                target_model_package)

        target_model_id = target_model.get('unique_id')

        if target_model_id not in model.get('depends_on', {}).get('nodes'):
            dbt.exceptions.ref_bad_context(model,
                                           target_model_name,
                                           target_model_package)

        is_ephemeral = (get_materialization(target_model) == 'ephemeral')

        if is_ephemeral:
            model.set_cte(target_model_id, None)
            return adapter.Relation.create(
                type=adapter.Relation.CTE,
                identifier=add_ephemeral_model_prefix(
                    target_model_name)).quote(identifier=False)
        else:
            return adapter.Relation.create_from_node(config, target_model)

    return do_ref


def source(db_wrapper, model, config, manifest):
    current_project = config.project_name

    def do_source(source_name, table_name):
        target_source = ParserUtils.resolve_source(
            manifest,
            source_name,
            table_name,
            current_project,
            model.get('package_name')
        )

        if target_source is None:
            dbt.exceptions.source_target_not_found(
                model,
                source_name,
                table_name)

        model.sources.append([source_name, table_name])
        return db_wrapper.Relation.create_from_source(target_source)

    return do_source


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

    def require(self, name, validator=None):
        if name not in self.model['config']:
            dbt.exceptions.missing_config(self.model, name)

        to_return = self.model['config'][name]

        if validator is not None:
            self._validate(validator, to_return)

        return to_return

    def get(self, name, validator=None, default=None):
        to_return = self.model['config'].get(name, default)

        if validator is not None and default is not None:
            self._validate(validator, to_return)

        return to_return


def generate(model, runtime_config, manifest):
    return dbt.context.common.generate(
        model, runtime_config, manifest, None, dbt.context.runtime)


def generate_macro(model, runtime_config, manifest, connection_name):
    return dbt.context.common.generate_execute_macro(
        model, runtime_config, manifest, dbt.context.runtime,
        connection_name
    )
