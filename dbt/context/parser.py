import dbt.utils
import dbt.exceptions

import dbt.context.common

from dbt.adapters.factory import get_adapter


execute = False


def ref(model, project, profile, flat_graph):

    def ref(*args):
        if len(args) == 1 or len(args) == 2:
            model['refs'].append(args)

        else:
            dbt.exceptions.ref_invalid_args(model, args)

        adapter = get_adapter(profile)
        return dbt.utils.Relation(profile, adapter, model)

    return ref


class Config:
    def __init__(self, model):
        self.model = model

    def __call__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.exceptions.raise_compiler_error(
                "Invalid inline model config",
                self.model)

        self.model['config_reference'].update_in_model_config(opts)
        return ''

    def set(self, name, value):
        return self.__call__({name: value})

    def require(self, name, validator=None):
        return ''

    def get(self, name, validator=None, default=None):
        return ''


def generate(model, project, flat_graph):
    return dbt.context.common.generate(
        model, project, flat_graph, dbt.context.parser)
