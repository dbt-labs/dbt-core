from dbt.adapters.factory import get_adapter

import dbt.clients.jinja
import dbt.flags
import dbt.utils

import dbt.context.common

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


def ref(model, project, profile, schema, flat_graph):
    current_project = project.get('name')

    def do_ref(*args):
        target_model_name = None
        target_model_package = None

        if len(args) == 1:
            target_model_name = args[0]
        elif len(args) == 2:
            target_model_package, target_model_name = args
        else:
            dbt.exceptions.ref_invalid_args(model, args)

        target_model = dbt.parser.resolve_ref(
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

        if dbt.utils.get_materialization(target_model) == 'ephemeral':
            model['extra_ctes'][target_model_id] = None
            return '__dbt__CTE__{}'.format(target_model.get('name'))
        else:
            adapter = get_adapter(profile)
            table = target_model.get('name')

            return adapter.quote_schema_and_table(profile, schema, table)

    return do_ref


def config(model):
    def do_config(*args, **kwargs):
        return ''

    return do_config


def generate(model, project, flat_graph):
    return dbt.context.common.generate(
        model, project, flat_graph, dbt.context.runtime)
