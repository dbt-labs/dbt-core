from dbt.utils import get_materialization, add_ephemeral_model_prefix

import dbt.clients.jinja
import dbt.context.common
import dbt.flags
import dbt.parser

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


execute = True


def ref(db_wrapper, model, project_cfg, profile, flat_graph):
    current_project = project_cfg.get('name')
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

        target_model = dbt.parser.ParserUtils.resolve_ref(
            flat_graph,
            target_model_name,
            target_model_package,
            current_project,
            model.get('package_name'))

        if target_model is None:
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
            model['extra_ctes'][target_model_id] = None
            return adapter.Relation.create(
                type=adapter.Relation.CTE,
                identifier=add_ephemeral_model_prefix(
                    target_model_name)).quote(identifier=False)
        else:
            return adapter.Relation.create_from_node(profile, target_model)

    return do_ref


class Config:
    def __init__(self, model):
        self.model = model

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


def generate(model, project_cfg, flat_graph):
    return dbt.context.common.generate(
        model, project_cfg, flat_graph, dbt.context.runtime)
