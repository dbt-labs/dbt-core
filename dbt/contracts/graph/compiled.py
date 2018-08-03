from copy import copy, deepcopy

from dbt.api import APIObject
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import deep_merge
from dbt.contracts.graph.parsed import PARSED_NODE_CONTRACT, \
    PARSED_MACRO_CONTRACT, ParsedNode

import dbt.compat

import sqlparse

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


def _inject_ctes_into_sql(sql, ctes):
    """
    `ctes` is a dict of CTEs in the form:

      {
        "cte_id_1": "__dbt__CTE__ephemeral as (select * from table)",
        "cte_id_2": "__dbt__CTE__events as (select id, type from events)"
      }

    Given `sql` like:

      "with internal_cte as (select * from sessions)
       select * from internal_cte"

    This will spit out:

      "with __dbt__CTE__ephemeral as (select * from table),
            __dbt__CTE__events as (select id, type from events),
            with internal_cte as (select * from sessions)
       select * from internal_cte"

    (Whitespace enhanced for readability.)
    """
    if len(ctes) == 0:
        return sql

    parsed_stmts = sqlparse.parse(sql)
    parsed = parsed_stmts[0]

    with_stmt = None
    for token in parsed.tokens:
        if token.is_keyword and token.normalized == 'WITH':
            with_stmt = token
            break

    if with_stmt is None:
        # no with stmt, add one, and inject CTEs right at the beginning
        first_token = parsed.token_first()
        with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
        parsed.insert_before(first_token, with_stmt)
    else:
        # stmt exists, add a comma (which will come after injected CTEs)
        trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ',')
        parsed.insert_after(with_stmt, trailing_comma)

    parsed.insert_after(
        with_stmt,
        sqlparse.sql.Token(sqlparse.tokens.Keyword, ", ".join(ctes.values())))

    return dbt.compat.to_string(parsed)


class CompiledNode(ParsedNode):
    SCHEMA = COMPILED_NODE_CONTRACT

    def prepend_ctes(self, prepended_ctes):
        self._contents['extra_ctes_injected'] = True
        self._contents['extra_ctes'] = prepended_ctes
        self._contents['injected_sql'] = _inject_ctes_into_sql(
            self.compiled_sql,
            prepended_ctes
        )

    @property
    def extra_ctes_injected(self):
        return self._contents.get('extra_ctes_injected')

    @property
    def extra_ctes(self):
        return self._contents.get('extra_ctes')

    @property
    def compiled(self):
        return self._contents.get('compiled')

    @compiled.setter
    def compiled(self, value):
        self._contents['compiled'] = value

    @property
    def injected_sql(self):
        return self._contents.get('injected_sql')

    @property
    def compiled_sql(self):
        return self._contents.get('compiled_sql')

    @compiled_sql.setter
    def compiled_sql(self, value):
        self._contents['compiled_sql'] = value

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
