from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length
from voluptuous.error import Invalid, MultipleInvalid

from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.graph.parsed import parsed_graph_item_contract

compiled_graph_item_contract = parsed_graph_item_contract.extend({
    # compiled fields
    Required('compiled'): bool,
    Required('compiled_sql'): Any(str, None),

    # injected fields
    Required('extra_ctes_injected'): bool,
    Required('extra_cte_ids'): All(list, [str]),
    Required('extra_cte_sql'): All(list, [str]),
    Required('injected_sql'): Any(str, None),
})


def validate_one(compiled_graph_item):
    try:
        compiled_graph_item_contract(compiled_graph_item)

    except Invalid as e:
        logger.info(e)
        raise ValidationException(str(e))


def validate(compiled_graph):
    try:
        for k, v in compiled_graph.items():
            compiled_graph_item_contract(v)

            if v.get('unique_id') != k:
                error_msg = 'unique_id must match key name in compiled graph!'
                logger.info(error_msg)
                raise ValidationException(error_msg)

    except Invalid as e:
        logger.info(e)
        raise ValidationException(str(e))
