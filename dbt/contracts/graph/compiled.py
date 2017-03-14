from voluptuous import Schema, Required, All, Any, Length

from collections import OrderedDict

from dbt.compat import basestring
from dbt.exceptions import ValidationException
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.common import validate_with
from dbt.contracts.graph.parsed import parsed_node_contract, \
    parsed_macro_contract


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


def validate_node(compiled_node):
    validate_with(compiled_node_contract, compiled_node)


def validate(compiled_graph):
    validate_with(compiled_graph_contract, compiled_graph)
