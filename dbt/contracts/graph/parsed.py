from voluptuous import Schema, Required, All, Any, Extra, Range, Optional, \
    Length

from dbt.compat import basestring
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.common import validate_with
from dbt.contracts.graph.unparsed import unparsed_graph_item_contract


config_contract = {
    Required('enabled'): bool,
    Required('materialized'): Any('table', 'view', 'ephemeral', 'incremental'),
    Required('post-hook'): list,
    Required('pre-hook'): list,
    Required('vars'): dict,

    # incremental optional fields
    Optional('sql_where'): basestring,
    Optional('unique_key'): basestring,

    # adapter optional fields
    Optional('sort'): basestring,
    Optional('dist'): basestring,
}


parsed_graph_item_contract = unparsed_graph_item_contract.extend({
    # identifiers
    Required('unique_id'): All(basestring, Length(min=1, max=255)),
    Required('fqn'): All(list, [All(basestring)]),

    # parsed fields
    Required('depends_on'): All(list,
                                [All(basestring, Length(min=1, max=255))]),
    Required('empty'): bool,
    Required('config'): config_contract,
    Required('tags'): All(list, [basestring]),
})


def validate_one(parsed_graph_item):
    validate_with(parsed_graph_item_contract, parsed_graph_item)

    materialization = parsed_graph_item.get('config', {}) \
                                       .get('materialized')

    if materialization == 'incremental' and \
       parsed_graph_item.get('config', {}).get('sql_where') is None:
        raise ValidationException(
            'missing `sql_where` for an incremental model')
    elif (materialization != 'incremental' and
          parsed_graph_item.get('config', {}).get('sql_where') is not None):
        raise ValidationException(
            'invalid field `sql_where` for a non-incremental model')


def validate(parsed_graph):
    for k, v in parsed_graph.items():
        validate_one(v)

        if v.get('unique_id') != k:
            error_msg = ('unique_id must match key name in parsed graph!'
                         'key: {}, model: {}'
                         .format(k, v))
            logger.info(error_msg)
            raise ValidationException(error_msg)
