from voluptuous import Schema, Required, All, Any, Length, ALLOW_EXTRA
from voluptuous import Optional

import dbt.exceptions

from dbt.compat import basestring
from dbt.utils import get_materialization
from dbt.node_types import NodeType

from dbt.contracts.common import validate_with
from dbt.contracts.graph.unparsed import unparsed_node_contract, \
    unparsed_base_contract

from dbt.logger import GLOBAL_LOGGER as logger  # noqa

hook_contract = Schema({
    Required('sql'): basestring,
    Required('transaction'): bool,
    Required('index'): int,
})

config_contract = Schema({
    Required('enabled'): bool,
    Required('materialized'): basestring,
    Required('post-hook'): [hook_contract],
    Required('pre-hook'): [hook_contract],
    Required('vars'): dict,
    Required('quoting'): dict,
    Required('column_types'): dict,
}, extra=ALLOW_EXTRA)

parsed_node_contract = unparsed_node_contract.extend({
    # identifiers
    Required('unique_id'): All(basestring, Length(min=1, max=255)),
    Required('fqn'): All(list, [All(basestring)]),
    Required('schema'): basestring,

    Required('refs'): [All(tuple)],

    # parsed fields
    Required('depends_on'): {
        Required('nodes'): [All(basestring, Length(min=1, max=255))],
        Required('macros'): [All(basestring, Length(min=1, max=255))],
    },

    Required('empty'): bool,
    Required('config'): config_contract,
    Required('tags'): All(set),

    # For csv files
    Optional('agate_table'): object,
})

parsed_nodes_contract = Schema({
    str: parsed_node_contract,
})

parsed_macro_contract = unparsed_base_contract.extend({
    # identifiers
    Required('resource_type'): Any(NodeType.Macro),
    Required('unique_id'): All(basestring, Length(min=1, max=255)),
    Required('tags'): All(set),

    # parsed fields
    Required('depends_on'): {
        Required('macros'): [All(basestring, Length(min=1, max=255))],
    },

    # contents
    Required('generator'): callable
})

parsed_macros_contract = Schema({
    str: parsed_macro_contract,
})


parsed_graph_contract = Schema({
    Required('nodes'): parsed_nodes_contract,
    Required('macros'): parsed_macros_contract,
})


def validate_hook(hook):
    validate_with(hook_contract, hooks)


def validate_nodes(parsed_nodes):
    validate_with(parsed_nodes_contract, parsed_nodes)


def validate_macros(parsed_macros):
    validate_with(parsed_macros_contract, parsed_macros)


def validate(parsed_graph):
    validate_with(parsed_graph_contract, parsed_graph)
