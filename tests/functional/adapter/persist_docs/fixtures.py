_MODELS__VIEW = """
{{ config(materialized='view') }}
select 2 as id, 'Bob' as name
"""

_MODELS__NO_DOCS_MODEL = """
select 1 as id, 'Alice' as name
"""

_DOCS__MY_FUN_DOCS = """
{% docs my_fun_doc %}
name Column description "with double quotes"
and with 'single  quotes' as welll as other;
'''abc123'''
reserved -- characters
80% of statistics are made up on the spot
--
/* comment */
Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

{% enddocs %}
"""

_MODELS__TABLE = """
{{ config(materialized='table') }}
select 1 as id, 'Joe' as name
"""


_MODELS__MISSING_COLUMN = """
{{ config(materialized='table') }}
select 1 as id, 'Ed' as name
"""

_MODELS__MODEL_USING_QUOTE_UTIL = """
select 1 as {{ adapter.quote("2id") }}
"""

_PROPERTIES__QUOTE_MODEL = """
version: 2
models:
  - name: quote_model
    description: "model to test column quotes and comments"
    columns:
      - name: 2id
        description: "XXX My description"
        quote: true
"""

_PROPERITES__SCHEMA_MISSING_COL = """
version: 2
models:
  - name: missing_column
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""

_PROPERTIES__SCHEMA_YML = """
version: 2

models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
  - name: view_model
    description: |
      View model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
"""


_SEEDS__SEED = """id,name
1,Alice
2,Bob
"""
