"""
This will be in the `dbt-prql` package, but including here during inital code review, so
we can test it without coordinating dependencies.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
import typing

if typing.TYPE_CHECKING:
    from dbt.parser.language_provider import references_type


# import prql_python
# This mocks the prqlc output for two cases which we currently use in tests, so we can
# test this without configuring dependencies. (Obv fix as we expand the tests, way
# before we merge.)
class prql_python:  # type: ignore
    @staticmethod
    def to_sql(prql) -> str:

        query_1 = "from employees"

        query_1_compiled = """
SELECT
  employees.*
FROM
  employees
        """.strip()

        query_2 = """
from (dbt source.salesforce.in_process)
join (dbt ref.foo.bar) [id]
filter salary > 100
        """.strip()

        query_2_refs_replaced = """
from (`{{ source('salesforce', 'in_process') }}`)
join (`{{ ref('foo', 'bar') }}`) [id]
filter salary > 100
        """.strip()

        query_2_compiled = """
SELECT
"{{ source('salesforce', 'in_process') }}".*,
"{{ ref('foo', 'bar') }}".*,
id
FROM
{{ source('salesforce', 'in_process') }}
JOIN {{ ref('foo', 'bar') }} USING(id)
WHERE
salary > 100
        """.strip()

        lookup = dict(
            {
                query_1: query_1_compiled,
                query_2: query_2_compiled,
                query_2_refs_replaced: query_2_compiled,
            }
        )

        return lookup[prql]


logger = logging.getLogger(__name__)

word_regex = r"[\w\.\-_]+"
references_regex = rf"\bdbt `?(\w+)\.({word_regex})\.({word_regex})`?"


def compile(prql: str, references: references_type):
    """
    >>> print(compile(
    ...     "from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar) [id]",
    ...     references=dict(
    ...         source={('salesforce', 'in_process'): 'salesforce_schema.in_process_tbl'},
    ...         ref={('foo', 'bar'): 'foo_schema.bar_tbl'}
    ...     )
    ... ))
    SELECT
      "{{ source('salesforce', 'in_process') }}".*,
      "{{ ref('foo', 'bar') }}".*,
      id
    FROM
      {{ source('salesforce', 'in_process') }}
      JOIN {{ ref('foo', 'bar') }} USING(id)
    """

    # references = list_references(prql)
    prql = _hack_sentinels_of_model(prql)

    sql = prql_python.to_sql(prql)

    # The intention was to replace the sentinels with table names. (Which would be done
    # by dbt-prql). But we now just pass the SQL off and let dbt handle it; something
    # expedient but not elegant.
    # sql = replace_faux_jinja(sql, references)
    return sql


def list_references(prql):
    """
    List all references (e.g. sources / refs) in a given block.

    We need to decide:

    — What should prqlc return given `dbt source.foo.bar`, so dbt-prql can find the
      references?
        — Should it just fill in something that looks like jinja for expediancy? (We
          don't support jinja though)

    >>> references = list_references("from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar)")
    >>> dict(references)
    {'source': [('salesforce', 'in_process')], 'ref': [('foo', 'bar')]}
    """

    out = defaultdict(list)
    for t, package, model in _hack_references_of_prql_query(prql):
        out[t] += [(package, model)]

    return out


def _hack_references_of_prql_query(prql) -> list[tuple[str, str, str]]:
    """
    List the references in a prql query.

    This would be implemented by prqlc.

    >>> _hack_references_of_prql_query("from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar)")
    [('source', 'salesforce', 'in_process'), ('ref', 'foo', 'bar')]
    """

    return re.findall(references_regex, prql)


def _hack_sentinels_of_model(prql: str) -> str:
    """
    Replace the dbt calls with a jinja-like sentinel.

    This will be done by prqlc...

    >>> _hack_sentinels_of_model("from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar) [id]")
    "from (`{{ source('salesforce', 'in_process') }}`) | join (`{{ ref('foo', 'bar') }}`) [id]"
    """
    return re.sub(references_regex, r"`{{ \1('\2', '\3') }}`", prql)


def replace_faux_jinja(sql: str, references: references_type):
    """
    >>> print(replace_faux_jinja(
    ...     "SELECT * FROM {{ source('salesforce', 'in_process') }}",
    ...     references=dict(source={('salesforce', 'in_process'): 'salesforce_schema.in_process_tbl'})
    ... ))
    SELECT * FROM salesforce_schema.in_process_tbl

    """
    for ref_type, lookups in references.items():
        for (package, table), literal in lookups.items():
            prql_new = sql.replace(f"{{{{ {ref_type}('{package}', '{table}') }}}}", literal)
            sql = prql_new

    return sql
