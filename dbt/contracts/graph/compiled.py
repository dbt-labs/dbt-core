from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length

from dbt.compat import basestring
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.common import validate_with
from dbt.contracts.graph.parsed import parsed_graph_item_contract


compiled_graph_item_contract = parsed_graph_item_contract.extend({
    # compiled fields
    Required('compiled'): bool,
    Required('compiled_sql'): Any(basestring, None),

    # injected fields
    Required('extra_ctes_injected'): bool,
    Required('extra_cte_ids'): All(list, [basestring]),
    Required('extra_cte_sql'): All(list, [basestring]),
    Required('injected_sql'): Any(basestring, None),
})


def validate_one(compiled_graph_item):
    validate_with(compiled_graph_item_contract, compiled_graph_item)


def validate(compiled_graph):
    for k, v in compiled_graph.items():
        validate_with(compiled_graph_item_contract, v)

        if v.get('unique_id') != k:
            error_msg = 'unique_id must match key name in compiled graph!'
            logger.info(error_msg)
            raise ValidationException(error_msg)
