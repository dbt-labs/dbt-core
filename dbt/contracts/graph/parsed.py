from dbt.api import APIObject
from dbt.utils import deep_merge
from dbt.node_types import NodeType

from dbt.contracts.graph.unparsed import UNPARSED_NODE_CONTRACT, \
    UNPARSED_BASE_CONTRACT

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


HOOK_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'sql': {
            'type': 'string',
        },
        'transaction': {
            'type': 'boolean',
        },
        'index': {
            'type': 'integer',
        }
    },
    'required': ['sql', 'transaction', 'index'],
}


CONFIG_CONTRACT = {
    'type': 'object',
    'additionalProperties': True,
    'properties': {
        'enabled': {
            'type': 'boolean',
        },
        'materialized': {
            'type': 'string',
        },
        'post-hook': {
            'type': 'array',
            'items': HOOK_CONTRACT,
        },
        'pre-hook': {
            'type': 'array',
            'items': HOOK_CONTRACT,
        },
        'vars': {
            'type': 'object',
            'additionalProperties': True,
        },
        'quoting': {
            'type': 'object',
            'additionalProperties': True,
        },
        'column_types': {
            'type': 'object',
            'additionalProperties': True,
        },
    },
    'required': [
        'enabled', 'materialized', 'post-hook', 'pre-hook', 'vars',
        'quoting', 'column_types'
    ]
}


PARSED_NODE_CONTRACT = deep_merge(
    UNPARSED_NODE_CONTRACT,
    {
        'properties': {
            'unique_id': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 255,
            },
            'fqn': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
            'schema': {
                'type': 'string',
                'description': (
                    'The actual database string that this will build into.'
                )
            },
            'refs': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'description': (
                        'The list of arguments passed to a single ref call.'
                    ),
                },
                'description': (
                    'The list of call arguments, one list of arguments per '
                    'call.'
                )
            },
            'depends_on': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'nodes': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 255,
                            'description': (
                                'A node unique ID that this depends on.'
                            )
                        }
                    },
                    'macros': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 255,
                            'description': (
                                'A macro unique ID that this depends on.'
                            )
                        }
                    },
                },
                'description': (
                    'A list of unique IDs for nodes and macros that this '
                    'node depends upon.'
                ),
                'required': ['nodes', 'macros'],
            },
            # TODO: move this into a class property.
            'empty': {
                'type': 'boolean',
                'description': 'True if the SQL is empty',
            },
            'config': CONFIG_CONTRACT,
            'tags': {
                'type': 'array',
                'items': {
                    'type': 'string',
                }
            },
            # TODO: Might be a python object? if so, class attr or something.
            'agate_table': {
                'type': 'object',
            },
        },
        'required': UNPARSED_NODE_CONTRACT['required'] + [
            'unique_id', 'fqn', 'schema', 'refs', 'depends_on', 'empty',
            'config', 'tags',
        ]
    }
)


PARSED_NODES_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': PARSED_NODE_CONTRACT
    },
}


PARSED_MACRO_CONTRACT = deep_merge(
    UNPARSED_BASE_CONTRACT,
    {
        'additionalProperties': False,
        'properties': {
            'resource_type': {
                'enum': [NodeType.Macro],
            },
            'unique_id': {
                'type': 'string',
                'minLength': 1,
                'maxLength': 255,
            },
            'tags': {
                'type': 'array',
                'items': {
                    'type': 'string',
                },
            },
            'depends_on': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'macros': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                            'minLength': 1,
                            'maxLength': 255,
                            'description': 'A single macro unique ID.'
                        }
                    }
                },
                'description': 'A list of all macros this macro depends on.',
                'required': ['macros'],
            }
        },
        'required': UNPARSED_BASE_CONTRACT['required'] + [
            'resource_type', 'unique_id', 'tags', 'depends_on'
        ]
    }
)

PARSED_MACROS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'patternProperties': {
        '.*': PARSED_MACRO_CONTRACT
    },
}

PARSED_GRAPH_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'nodes': PARSED_NODES_CONTRACT,
        'macros': PARSED_MACROS_CONTRACT,
    },
    'required': ['nodes', 'macros'],
}


class ParsedManifest(APIObject):
    SCHEMA = PARSED_NODES_CONTRACT


class Hook(APIObject):
    SCHEMA = HOOK_CONTRACT


class ParsedMacros(APIObject):
    SCHEMA = PARSED_MACROS_CONTRACT


class ParsedGraph(APIObject):
    SCHEMA = PARSED_GRAPH_CONTRACT
