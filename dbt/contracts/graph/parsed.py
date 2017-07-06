from voluptuous import Schema, Required, All, Any, Length, Optional

import jinja2.runtime

import dbt.exceptions

from dbt.compat import basestring
from dbt.utils import get_materialization
from dbt.node_types import NodeType

from dbt.contracts.common import validate_with
from dbt.contracts.graph.unparsed import unparsed_node_contract, \
    unparsed_base_contract

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


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
    Optional('sort'): Any(basestring, list),
    Optional('dist'): basestring,

    Optional('sort_type'): Any('compound', 'interleaved'),
}

parsed_node_contract = unparsed_node_contract.extend({
    # identifiers
    Required('unique_id'): All(basestring, Length(min=1, max=255)),
    Required('fqn'): All(list, [All(basestring)]),

    Required('refs'): [All(tuple)],

    # parsed fields
    Required('depends_on'): {
        Required('nodes'): [All(basestring, Length(min=1, max=255))],
        Required('macros'): [All(basestring, Length(min=1, max=255))],
    },

    Required('empty'): bool,
    Required('config'): config_contract,
    Required('tags'): All(set),
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
    Required('parsed_macro'): jinja2.runtime.Macro

})

parsed_macros_contract = Schema({
    str: parsed_macro_contract,
})


parsed_graph_contract = Schema({
    Required('nodes'): parsed_nodes_contract,
    Required('macros'): parsed_macros_contract,
})


def validate_nodes(parsed_nodes):
    validate_with(parsed_nodes_contract, parsed_nodes)

    [validate_incremental(node) for unique_id, node
     in parsed_nodes.items()]


def validate_macros(parsed_macros):
    validate_with(parsed_macros_contract, parsed_macros)


def validate(parsed_graph):
    validate_with(parsed_graph_contract, parsed_graph)

    [validate_incremental(node) for unique_id, node
     in parsed_graph.get('nodes').items()]


def validate_incremental(node):
    if(node.get('resource_type') == NodeType.Model and
       get_materialization(node) == 'incremental' and
       node.get('config', {}).get('sql_where') is None):
        dbt.exceptions.missing_sql_where(node)
