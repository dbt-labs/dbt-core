from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length

from dbt.contracts.common import validate_with
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
    for item in unparsed_graph:
        validate_with(unparsed_graph_item_contract, item)
