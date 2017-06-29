from dbt.utils import get_materialization, compiler_error
from dbt.adapters.factory import get_adapter
from dbt.compat import basestring

import dbt.clients.jinja
import dbt.flags

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


def macro_generator(template, name):
    def apply_context(context):
        def call(*args, **kwargs):
            module = template.make_module(
                context, False, {})
            macro = module.__dict__[name]
            module.__dict__ = context
            return macro(*args, **kwargs)

        return call
    return apply_context
