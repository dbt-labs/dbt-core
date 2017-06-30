import dbt.utils
import dbt.exceptions

import dbt.context.common


def ref(model, project, profile, schema, flat_graph):

    def ref(*args):
        if len(args) == 1 or len(args) == 2:
            model['refs'].append(args)

        else:
            dbt.exceptions.ref_invalid_args(model, args)

    return ref


def config(model):

    def config(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.utils.compiler_error(
                model.get('name'),
                "Invalid model config given inline in {}".format(model))

        model['config_reference'].update_in_model_config(opts)

    return config


def generate(model, project, flat_graph):
    return dbt.context.common.generate(
        model, project, flat_graph, dbt.context.parser)
