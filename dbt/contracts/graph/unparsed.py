from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length

from dbt.compat import basestring
from dbt.contracts.common import validate_with
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.model import NodeType

unparsed_graph_item_contract = Schema({
    # identifiers
    Required('name'): All(basestring, Length(min=1, max=63)),
    Required('package_name'): basestring,
    Required('resource_type'): Any(NodeType.Model,
                                   NodeType.Test,
                                   NodeType.Analysis),

    # filesystem
    Required('root_path'): basestring,
    Required('path'): basestring,
    Required('raw_sql'): basestring,
})


def validate(unparsed_graph):
    for item in unparsed_graph:
        validate_with(unparsed_graph_item_contract, item)
