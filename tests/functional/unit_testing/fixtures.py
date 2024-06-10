import pytest

my_model_vars_sql = """
SELECT
a+b as c,
concat(string_a, string_b) as string_c,
not_testing, date_a,
{{ dbt.string_literal(type_numeric()) }} as macro_call,
{{ dbt.string_literal(var('my_test')) }} as var_call,
{{ dbt.string_literal(env_var('MY_TEST', 'default')) }} as env_var_call,
{{ dbt.string_literal(invocation_id) }} as invocation_id
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_sql = """
SELECT
a+b as c,
concat(string_a, string_b) as string_c,
not_testing, date_a
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_a_sql = """
SELECT
1 as a,
1 as id,
2 as not_testing,
'a' as string_a,
DATE '2020-01-02' as date_a
"""

my_model_b_sql = """
SELECT
2 as b,
1 as id,
2 as c,
'b' as string_b
"""

my_model_check_null_sql = """
SELECT
CASE
  WHEN a IS null THEN True
  ELSE False
END a_is_null
FROM {{ ref('my_model_a') }}
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 2}

  - name: test_my_model_empty
    model: my_model
    given:
      - input: ref('my_model_a')
        rows: []
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows: []

  - name: test_my_model_overrides
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    overrides:
      macros:
        type_numeric: override
        invocation_id: 123
      vars:
        my_test: var_override
      env_vars:
        MY_TEST: env_var_override
    expect:
      rows:
        - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}

  - name: test_my_model_string_concat
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, string_a: a}
      - input: ref('my_model_b')
        rows:
          - {id: 1, string_b: b}
    expect:
      rows:
        - {string_c: ab}
    config:
        tags: test_this
"""

test_my_model_pass_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 3}
"""


test_my_model_simple_fixture_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 2}

  - name: test_depends_on_fixture
    model: my_model
    given:
      - input: ref('my_model_a')
        rows: []
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_fixture
    expect:
      rows: []

  - name: test_my_model_overrides
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    overrides:
      macros:
        type_numeric: override
        invocation_id: 123
      vars:
        my_test: var_override
      env_vars:
        MY_TEST: env_var_override
    expect:
      rows:
        - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}

  - name: test_has_string_c_ab
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, string_a: a}
      - input: ref('my_model_b')
        rows:
          - {id: 1, string_b: b}
    expect:
      rows:
        - {string_c: ab}
    config:
        tags: test_this
"""


datetime_test = """
  - name: test_my_model_datetime
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, date_a: "2020-01-01"}
      - input: ref('my_model_b')
        rows:
          - {id: 1}
    expect:
      rows:
        - {date_a: "2020-01-01"}
"""

event_sql = """
select DATE '2020-01-01' as event_time, 1 as event
union all
select DATE '2020-01-02' as event_time, 2 as event
union all
select DATE '2020-01-03' as event_time, 3 as event
"""

datetime_test_invalid_format_key = """
  - name: test_my_model_datetime
    model: my_model
    given:
      - input: ref('my_model_a')
        format: xxxx
        rows:
          - {id: 1, date_a: "2020-01-01"}
      - input: ref('my_model_b')
        rows:
          - {id: 1}
    expect:
      rows:
        - {date_a: "2020-01-01"}
"""

datetime_test_invalid_csv_values = """
  - name: test_my_model_datetime
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows:
          - {id: 1, date_a: "2020-01-01"}
      - input: ref('my_model_b')
        rows:
          - {id: 1}
    expect:
      rows:
        - {date_a: "2020-01-01"}
"""

datetime_test_invalid_csv_file_values = """
  - name: test_my_model_datetime
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows:
          - {id: 1, date_a: "2020-01-01"}
      - input: ref('my_model_b')
        rows:
          - {id: 1}
    expect:
      rows:
        - {date_a: "2020-01-01"}
"""

event_sql = """
select DATE '2020-01-01' as event_time, 1 as event
union all
select DATE '2020-01-02' as event_time, 2 as event
union all
select DATE '2020-01-03' as event_time, 3 as event
"""

my_incremental_model_sql = """
{{
    config(
        materialized='incremental'
    )
}}

select * from {{ ref('events') }}
{% if is_incremental() %}
where event_time > (select max(event_time) from {{ this }})
{% endif %}
"""

test_my_model_incremental_yml_basic = """
unit_tests:
  - name: incremental_false
    model: my_incremental_model
    overrides:
      macros:
        is_incremental: false
    given:
      - input: ref('events')
        rows:
          - {event_time: "2020-01-01", event: 1}
    expect:
      rows:
        - {event_time: "2020-01-01", event: 1}
  - name: incremental_true
    model: my_incremental_model
    overrides:
      macros:
        is_incremental: true
    given:
      - input: ref('events')
        rows:
          - {event_time: "2020-01-01", event: 1}
          - {event_time: "2020-01-02", event: 2}
          - {event_time: "2020-01-03", event: 3}
      - input: this
        rows:
          - {event_time: "2020-01-01", event: 1}
    expect:
      rows:
        - {event_time: "2020-01-02", event: 2}
        - {event_time: "2020-01-03", event: 3}
"""

test_my_model_incremental_yml_no_override = """
unit_tests:
  - name: incremental_false
    model: my_incremental_model
    given:
      - input: ref('events')
        rows:
          - {event_time: "2020-01-01", event: 1}
    expect:
      rows:
        - {event_time: "2020-01-01", event: 1}
"""

test_my_model_incremental_yml_wrong_override = """
unit_tests:
  - name: incremental_false
    model: my_incremental_model
    overrides:
      macros:
        is_incremental: foobar
    given:
      - input: ref('events')
        rows:
          - {event_time: "2020-01-01", event: 1}
    expect:
      rows:
        - {event_time: "2020-01-01", event: 1}
"""

test_my_model_incremental_yml_no_this_input = """
unit_tests:
  - name: incremental_true
    model: my_incremental_model
    overrides:
      macros:
        is_incremental: true
    given:
      - input: ref('events')
        rows:
          - {event_time: "2020-01-01", event: 1}
          - {event_time: "2020-01-02", event: 2}
          - {event_time: "2020-01-03", event: 3}
    expect:
      rows:
        - {event_time: "2020-01-02", event: 2}
        - {event_time: "2020-01-03", event: 3}
"""

# -- inline csv tests

test_my_model_csv_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      rows: |
        c
        2

  - name: test_my_model_empty
    model: my_model
    given:
      - input: ref('my_model_a')
        rows: []
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      rows: []
  - name: test_my_model_overrides
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    overrides:
      macros:
        type_numeric: override
        invocation_id: 123
      vars:
        my_test: var_override
      env_vars:
        MY_TEST: env_var_override
    expect:
      rows:
        - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}
  - name: test_my_model_string_concat
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,string_a
          1,a
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,string_b
          1,b
    expect:
      format: csv
      rows: |
        string_c
        ab
    config:
        tags: test_this
"""

# -- csv file tests
test_my_model_file_csv_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_numeric_fixture
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_fixture
    expect:
      format: csv
      fixture: test_my_model_basic_fixture

  - name: test_my_model_empty
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_empty_fixture
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_fixture
    expect:
      format: csv
      fixture: test_my_model_a_empty_fixture

  - name: test_my_model_overrides
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_numeric_fixture
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_fixture
    overrides:
      macros:
        type_numeric: override
        invocation_id: 123
      vars:
        my_test: var_override
      env_vars:
        MY_TEST: env_var_override
    expect:
      rows:
        - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}

  - name: test_my_model_string_concat
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_fixture
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_b_fixture
    expect:
      format: csv
      fixture: test_my_model_concat_fixture
    config:
      tags: test_this
"""

test_my_model_fixture_csv = """id,b
1,2
2,2
"""

test_my_model_a_fixture_csv = """id,string_a
1,a
"""

test_my_model_a_with_null_fixture_csv = """id,a
1,
2,3
"""

test_my_model_a_empty_fixture_csv = """
"""

test_my_model_a_numeric_fixture_csv = """id,a
1,1
"""

test_my_model_b_fixture_csv = """id,string_b
1,b
"""

test_my_model_basic_fixture_csv = """c
2
"""

test_my_model_concat_fixture_csv = """string_c
ab
"""

# -- mixed inline and file csv
test_my_model_mixed_csv_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      fixture: test_my_model_basic_fixture

  - name: test_my_model_empty
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_empty_fixture
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      fixture: test_my_model_a_empty_fixture

  - name: test_my_model_overrides
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_fixture
    overrides:
      macros:
        type_numeric: override
        invocation_id: 123
      vars:
        my_test: var_override
      env_vars:
        MY_TEST: env_var_override
    expect:
      rows:
        - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}

  - name: test_my_model_string_concat
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_fixture
      - input: ref('my_model_b')
        format: csv
        fixture: test_my_model_b_fixture
    expect:
      format: csv
      rows: |
        string_c
        ab
    config:
      tags: test_this
"""

# unit tests with errors

# -- fixture file doesn't exist
test_my_model_missing_csv_yml = """
unit_tests:
  - name: test_missing_csv_file
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      fixture: fake_fixture
"""

test_my_model_duplicate_csv_yml = """
unit_tests:
  - name: test_missing_csv_file
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      fixture: test_my_model_basic_fixture
"""

test_model_a_b_yml = """
unit_tests:
  - name: my_test_name
    model: my_model_a
    given: []
    expect:
      rows:
        - {a: 1, id: 1, not_testing: 2, string_a: "a", date_a: "2020-01-02"}

  - name: my_test_name
    model: my_model_b
    given: []
    expect:
      rows:
        - {b: 2, id: 1, c: 2, string_b: "b"}
"""

test_model_a_with_duplicate_test_name_yml = """
unit_tests:
  - name: my_test_name
    model: my_model_a
    given: []
    expect:
      rows:
        - {a: 1, id: 1, not_testing: 2, string_a: "a", date_a: "2020-01-02"}

  - name: my_test_name
    model: my_model_a
    given: []
    expect:
      rows:
        - {a: 1, id: 1, not_testing: 2, string_a: "a", date_a: "2020-01-02"}
"""

test_my_model_yml_invalid = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: "a"}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 3}
"""

test_my_model_yml_invalid_ref = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_x')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 3}
"""

# -- unit testing versioned models
my_model_v1_sql = """
SELECT
a,
b,
a+b as c,
concat(string_a, string_b) as string_c,
not_testing, date_a
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_v2_sql = """
SELECT
a,
b,
a+b as c,
concat(string_a, string_b) as string_c,
date_a
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_v3_sql = """
SELECT
a,
b,
a+b as c,
concat(string_a, string_b) as string_c
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_versioned_yml = """
models:
  - name: my_model
    latest_version: 1
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: a
        data_type: integer
      - name: b
        data_type: integer
      - name: c
        data_type: integer
      - name: string_c
        data_type: string
      - name: not_testing
        data_type: integer
      - name: date_a
        data_type: date
    versions:
      - v: 1
      - v: 2
        columns:
          # This means: use the 'columns' list from above, but exclude not_testing
          - include: "all"
            exclude:
            - not_testing
      - v: 3
        # now exclude another column
        columns:
          - include: all
            exclude:
            - not_testing
            - date_a
"""

my_model_versioned_no_2_yml = """
models:
  - name: my_model
    latest_version: 1
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: a
        data_type: integer
      - name: b
        data_type: integer
      - name: c
        data_type: integer
      - name: string_c
        data_type: string
      - name: not_testing
        data_type: integer
      - name: date_a
        data_type: date
    versions:
      - v: 1
      - v: 3
        # now exclude another column
        columns:
          - include: all
            exclude:
            - not_testing
            - date_a
"""

test_my_model_all_versions_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
          2,3
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      rows: |
          a,b,c
          1,2,3
          3,2,5
"""

test_my_model_exclude_versions_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    versions:
      exclude:
        - 2
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
          2,3
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      rows: |
          a,b,c
          1,2,3
          3,2,5
"""

test_my_model_include_versions_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    versions:
      include:
        - 2
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
          2,3
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      rows: |
          a,b,c
          1,2,3
          3,2,5
"""

test_my_model_include_exclude_versions_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    versions:
      include:
        - 2
      exclude:
        - 3
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,1
          2,3
      - input: ref('my_model_b')
        format: csv
        rows: |
          id,b
          1,2
          2,2
    expect:
      format: csv
      rows: |
          a,b,c
          1,2,3
          3,2,5
"""

test_my_model_include_unversioned_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    versions:
      include:
        - 2
    given:
      - input: ref('my_model_a')
        rows:
          - {id: 1, a: 1}
      - input: ref('my_model_b')
        rows:
          - {id: 1, b: 2}
          - {id: 2, b: 2}
    expect:
      rows:
        - {c: 2}
"""

my_model_version_ref_sql = """
   select * from {{ ref('my_model', version=2) }}
"""

test_my_model_version_ref_yml = """
unit_tests:
  - name: test_my_model_version_ref
    model: my_model_version_ref
    given:
      - input: ref('my_model', version=2)
        rows:
          - {c: 2}
    expect:
      rows:
        - {c: 2}
"""


# -- unit testing external models
top_level_domains_sql = """
SELECT 'example.com' AS tld
UNION ALL
SELECT 'gmail.com' AS tld
"""

valid_emails_sql = """
WITH
accounts AS (
  SELECT user_id, email, email_top_level_domain
  FROM {{ ref('external_package', 'external_model')}}
),
top_level_domains AS (
  SELECT tld FROM {{ ref('top_level_domains')}}
),
joined AS (
  SELECT
    accounts.user_id as user_id,
    top_level_domains.tld as tld
  FROM accounts
  LEFT OUTER JOIN top_level_domains
    ON   accounts.email_top_level_domain = top_level_domains.tld
)

SELECT
  joined.user_id as user_id,
  CASE WHEN joined.tld IS NULL THEN FALSE ELSE TRUE END AS is_valid_email_address
from joined
"""

external_package__accounts_seed_csv = """user_id,email,email_top_level_domain
1,"example@example.com","example.com"
"""

external_package__external_model_sql = """
SELECT user_id, email, email_top_level_domain FROM {{ ref('accounts_seed') }}
"""


external_package_project_yml = """
name: external_package
version: '1.0'
config-version: 2

model-paths: ["models"]    # paths to models
analysis-paths: ["analyses"] # path with analysis files which are compiled, but not run
target-path: "target"      # path for compiled code
clean-targets: ["target"]  # directories removed by the clean task
test-paths: ["tests"]       # where to store test results
seed-paths: ["seeds"]       # load CSVs from this directory with `dbt seed`
macro-paths: ["macros"]    # where to find macros

profile: user

models:
    external_package:
"""


@pytest.fixture(scope="class")
def external_package():
    return {
        "dbt_project.yml": external_package_project_yml,
        "seeds": {"accounts_seed.csv": external_package__accounts_seed_csv},
        "models": {
            "external_model.sql": external_package__external_model_sql,
        },
    }


model_select_1_sql = """
select 1 as id
"""

model_select_2_sql = """
select 2 as id
"""

test_expect_2_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given: []
    expect:
      rows:
        - {id: 2}
"""


test_my_model_csv_null_yml = """
unit_tests:
  - name: test_my_model_check_null
    model: my_model_check_null
    given:
      - input: ref('my_model_a')
        format: csv
        rows: |
          id,a
          1,
          2,3
    expect:
      format: csv
      rows: |
        a_is_null
        True
        False
"""

test_my_model_file_csv_null_yml = """
unit_tests:
  - name: test_my_model_check_null
    model: my_model_check_null
    given:
      - input: ref('my_model_a')
        format: csv
        fixture: test_my_model_a_with_null_fixture
    expect:
      format: csv
      rows: |
        a_is_null
        True
        False
"""
