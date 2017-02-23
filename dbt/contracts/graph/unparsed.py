from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length
from voluptuous.error import Invalid, MultipleInvalid

from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

unparsed_graph_item_contract = Schema({
    # identifiers
    Required('name'): All(str, Length(min=1, max=63)),
    Required('package_name'): str,

    # filesystem
    Required('root_path'): str,
    Required('path'): str,
    Required('raw_sql'): str,
})


def validate(unparsed_graph):
    try:
        for item in unparsed_graph:
            unparsed_graph_item_contract(item)

    except Invalid as e:
        logger.info(e)
        raise ValidationException(str(e))
