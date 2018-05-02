from voluptuous import Schema, Required, All, Any, Length

from collections import OrderedDict

from dbt.api import APIObject
from dbt.compat import basestring
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import deep_merge

from dbt.contracts.common import validate_with
from dbt.contracts.graph.parsed import parsed_node_contract, \
    parsed_macro_contract
from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT

compiled_node_contract = parsed_node_contract.extend({
    # compiled fields
    Required('compiled'): bool,
    Required('compiled_sql'): Any(basestring, None),

    # injected fields
    Required('extra_ctes_injected'): bool,
    Required('extra_ctes'): All(OrderedDict, {
        basestring: Any(basestring, None)
    }),
    Required('injected_sql'): Any(basestring, None),
})

# this is the only way I know to do OrderedDict equivalent in JSON.
ORDERED_DICT_CONTRACT = {
    'type': 'array',
    'items': {
        'type': 'object',
        'additionalProperties': False,
        'required': ['key', 'value'],
        'properties': {
            'key': {
                'type': 'string',
            },
            'value': {
                'type': ['string', 'null']
            },
        },
    },
}

COMPILED_NODE_CONTRACT = deep_merge(
    PARSED_NODE_CONTRACT,
    {
        'properties': {
            'compiled': {
                'type': 'boolean'
            },
            'compiled_sql': {
                'type': ['string', 'null'],
            },
            'extra_ctes_injected': {
                'type': 'boolean',
            },
            'extra_ctes': ORDERED_DICT_CONTRACT,
            'injected_sql': {
                'type': ['string', 'null'],
            },
        },
        'required': PARSED_NODE_CONTRACT['required'] + [
            'compiled', 'compiled_sql', 'extra_ctes_injected', 'extra_ctes',
            'injected_sql'
        ]
    }
)


class CompiledNode(APIObject):
    SCHEMA = COMPILED_NODE_CONTRACT


compiled_nodes_contract = Schema({
    str: compiled_node_contract,
})

compiled_macro_contract = parsed_macro_contract

compiled_macros_contract = Schema({
    str: compiled_macro_contract,
})

compiled_graph_contract = Schema({
    Required('nodes'): compiled_nodes_contract,
    Required('macros'): compiled_macros_contract,
})


def validate(compiled_graph):
    validate_with(compiled_graph_contract, compiled_graph)
