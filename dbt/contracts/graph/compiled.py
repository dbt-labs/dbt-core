from copy import copy, deepcopy

from dbt.api import APIObject
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import deep_merge

from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT, \
    PARSED_MACRO_CONTRACT, ParsedNode

COMPILED_NODE_CONTRACT = deep_merge(
    PARSED_NODE_CONTRACT,
    {
        # TODO: when we add 'extra_ctes' back in, flip this back to False
        'additionalProperties': True,
        'properties': {
            'compiled': {
                'description': (
                    'This is true after the node has been compiled, but ctes '
                    'have not necessarily been injected into the node.'
                ),
                'type': 'boolean'
            },
            'compiled_sql': {
                'type': ['string', 'null'],
            },
            'extra_ctes_injected': {
                'description': (
                    'This is true after extra ctes have been injected into '
                    'the compiled node.'
                ),
                'type': 'boolean',
            },
            # TODO: properly represent this so we can serialize/deserialize and
            # preserve order.
            'extra_ctes': {
                'type': 'object',
                'additionalProperties': True,
                'description': 'The injected CTEs for a model'
            },
            'injected_sql': {
                'type': ['string', 'null'],
                'description': 'The SQL after CTEs have been injected',
            },
            'wrapped_sql': {
                'type': ['string', 'null'],
                'description': (
                    'The SQL after it has been wrapped (for tests, '
                    'operations, and analysis)'
                ),
            },
        },
        'required': PARSED_NODE_CONTRACT['required'] + [
            'compiled', 'compiled_sql', 'extra_ctes_injected',
            'injected_sql', 'extra_ctes'
        ]
    }
)

COMPILED_NODES_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the compiled nodes, stored by their unique IDs.'
    ),
    'patternProperties': {
        '.*': COMPILED_NODE_CONTRACT
    },
}

COMPILED_MACRO_CONTRACT = PARSED_MACRO_CONTRACT

COMPILED_MACROS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'A collection of the compiled macros, stored by their unique IDs.'
    ),
    'patternProperties': {
        '.*': COMPILED_MACRO_CONTRACT
    },
}

COMPILED_GRAPH_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'description': (
        'The full compiled graph, with both the required nodes and required '
        'macros.'
    ),
    'properties': {
        'nodes': COMPILED_NODES_CONTRACT,
        'macros': COMPILED_MACROS_CONTRACT,
    },
    'required': ['nodes', 'macros'],
}


class CompiledNode(ParsedNode):
    SCHEMA = COMPILED_NODE_CONTRACT

    @property
    def extra_ctes_injected(self):
        return self._contents.get('extra_ctes_injected')

    @extra_ctes_injected.setter
    def extra_ctes_injected(self, value):
        self._contents['extra_ctes_injected'] = value

    @property
    def extra_ctes(self):
        return self._contents.get('extra_ctes')

    @extra_ctes.setter
    def extra_ctes(self, value):
        self._contents['extra_ctes'] = value

    @property
    def injected_sql(self):
        return self._contents.get('injected_sql')

    @injected_sql.setter
    def injected_sql(self, value):
        self._contents['injected_sql'] = value

    @property
    def compiled(self):
        return self._contents.get('compiled')

    @compiled.setter
    def compiled(self, value):
        self._contents['compiled'] = value

    @property
    def compiled_sql(self):
        return self._contents.get('compiled_sql')

    @compiled_sql.setter
    def compiled_sql(self, value):
        self._contents['compiled_sql'] = value

    @property
    def injected_sql(self):
        return self._contents.get('injected_sql')

    @injected_sql.setter
    def injected_sql(self, value):
        self._contents['injected_sql'] = value

    @property
    def wrapped_sql(self):
        return self._contents.get('wrapped_sql')

    @wrapped_sql.setter
    def wrapped_sql(self, value):
        self._contents['wrapped_sql'] = value

    def to_dict(self):
        # if we have ctes, we want to preseve order, so deepcopy them.
        ret = super(CompiledNode, self).to_dict()
        ret['extra_ctes'] = deepcopy(self.extra_ctes)
        return ret

    def to_shallow_dict(self):
        ret = super(CompiledNode, self).to_shallow_dict()
        ret['extra_ctes'] = self.extra_ctes
        return ret


class CompiledGraph(APIObject):
    SCHEMA = COMPILED_GRAPH_CONTRACT
