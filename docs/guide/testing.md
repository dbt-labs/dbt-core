# Testing

Note: We believe that data testing and validation is an essential part of a mature analytical workflow. We anticipate spending much more time working on these features once the core modeling, templating, and scheduling functionality is completed.

dbt provides two different mechanisms for data validation: schema tests and custom data tests.

## Schema tests

Data integrity in analytic databases is empirically often of lower quality than data in transactional systems. Schema testing provides users a repeatable way to ensure that their schema adheres to basic rules: referential integrity, uniqueness, etc. Building schema tests and running them on an ongoing basis gives users of the resulting data confidence that analytic queries produce the desired outputs.

Tests are run with `dbt test`. See [usage](usage/) for more information on the dbt command structure. `dbt test` will report back the success or failure of each test, and in case of failure will report the number of failing rows.

Schema tests are declared in a `schema.yml` file that can be placed at any level within your models folders. See the sample provided [here](https://github.com/fishtown-analytics/dbt/blob/master/sample.schema.yml). There are four primary schema validations provided.

### Not null

Validates that there are no null values present in a field.

```YAML
people:
  constraints:
    not_null:
      - id
      - account_id
      - name
```

### Unique

Validates that there are no duplicate values present in a field.

```YAML
people:
  constraints:
    unique:
      - id
```

### Relationships

This validates that all records in a child table have a corresponding record in a parent table. For example, the following tests that all `account_id`s in `people` have a corresponding `id` in `accounts`.

```YAML
people:
  constraints:
    relationships:
      - {from: account_id, to: accounts, field: id}
```

### Accepted values

This validates that all of the values in a given field are present in the list supplied. Any values other than those provided in the list will fail the test.

```YAML
people:
  constraints:
    accepted_values:
      - {field: status, values: ['active', 'cancelled']}
```

It is recommended that users specify tests for as many constraints as can be reasonably identified in their database. This may result in a large number of total tests, but `schema.yml` makes it fast to create and modify these tests, and the presence of additional tests of this sort can significantly increase the confidence in underlying data consistency in a database.


## Custom data tests

Not all error conditions can be expressed in a schema test. For this reason, dbt provides a mechanism for testing arbitrary assertions about your data. These data tests are sql `SELECT` statements that return 0 rows on success, or > 0 rows on failure.

A typical data test might look like:

```sql
-- ./tests/assert_less_than_5_pct_event_cookie_ids_are_null.sql

-- If >= 5% of cookie_ids are null, then the test returns 1 row (failure).
-- If < 5% of cookie_ids are null, then the test returns 0 rows (success)

with calc as (

    select
      sum(case when cookie_id is null then 1 else 0 end)::float / count(*)::float as fraction
    from {{ ref('events') }}

)

select * from calc where fraction < 0.05
```

To enable data tests, add the `test-paths` config to your `dbt_project.yml` file:


```yml
# dbt_project.yml

name: 'Vandelay Industries`
version: '1.0'

source-paths: ["models"]
target-path: "target"
test-paths: ["tests"]        # look for *.sql files in the "tests" directory
...
```

Any .sql files found in the `test-paths` directories will be evaluated as data tests. These tests can be run with:

```bash
$ dbt test         # run schema + data tests
$ dbt test --data  # run only data tests
```
