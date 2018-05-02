from dbt.api import APIObject
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import deep_merge

from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT, \
    PARSED_MACRO_CONTRACT

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
            'extra_ctes': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
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

COMPILED_NODES_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': COMPILED_NODE_CONTRACT
    },
}

COMPILED_MACRO_CONTRACT = PARSED_MACRO_CONTRACT

COMPILED_MACROS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': COMPILED_MACRO_CONTRACT
    },
}

COMPILED_GRAPH_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'nodes': COMPILED_NODES_CONTRACT,
        'macros': COMPILED_MACROS_CONTRACT,
    },
    'required': ['nodes', 'macros'],
}


class CompiledNode(APIObject):
    SCHEMA = COMPILED_NODE_CONTRACT


class CompiledGraph(APIObject):
    SCHEMA = COMPILED_GRAPH_CONTRACT
