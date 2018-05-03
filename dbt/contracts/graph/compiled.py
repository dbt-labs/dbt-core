from dbt.api import APIObject
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import deep_merge

from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT, \
    PARSED_MACRO_CONTRACT

COMPILED_NODE_CONTRACT = deep_merge(
    PARSED_NODE_CONTRACT,
    {
        # TODO: when we add 'extra_ctes' back in, flip this back to False
        'additionalProperties': True,
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
            # TODO: add this back in, and add back to 'required' list
            # 'extra_ctes': {
            #     'type': 'array',
            #     'items': {
            #         'type': 'string',
            #     }
            # },
            'injected_sql': {
                'type': ['string', 'null'],
            },
        },
        'required': PARSED_NODE_CONTRACT['required'] + [
            'compiled', 'compiled_sql', 'extra_ctes_injected',
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
    'serialize': ['nodes'],
}


class CompiledNode(APIObject):
    SCHEMA = COMPILED_NODE_CONTRACT


class CompiledGraph(APIObject):
    SCHEMA = COMPILED_GRAPH_CONTRACT
