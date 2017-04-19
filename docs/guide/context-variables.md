# Context variables

dbt makes available certain variables for developers to use to control a model's behavior. These variables may be defined within a certain model scope within the model hierarchy or may be global.

## this

`this` returns the schema and table name of the currently executing model. This is useful in any context in which you need to write code that references the current model, for example when defining a `sql_where` clause for an incremental model and for writing pre- and post-model hooks that operate on the model in some way. Developers have options for how to use `this`:

| Context Variable | Value                                                                                                                                                                                                      |
|------------------|-----------------------------------------|
| this             | Returns "schema"."table"                                                                                                                                                                                   |
| this.schema      | Returns schema                                                                                                                                                                                             |
| this.table       | Returns the name of the table that is being operated on in the database transaction. Sometimes dbt creates temporary tables; if that is true in this instance, then temporary table name will be returned. |
| this.name        | Returns the logical table name (ignores temporary tables)

In most cases, the correct choice will be to simply use `this`, although at times the other derivatives will be useful. Developers are left to choose what is most appropriate.

Here's an example of how to use `this` to write a `sql_where` clause that only inserts the new records since the most recent timestamp found in the table:

```yml
sessions:
  materialized: incremental
  sql_where: "session_end_tstamp > (select max(session_end_tstamp) from {{this}})"
```


Here's an example of how to use `this` to grant select rights on a table to a different db user.

```yml
models:
  project-name:
    post-hook:
      - "grant select on {{ this }} to db_reader"
```

Besides `{{ this }}`, there are a number of other helpful context variables available for use in modeling code

| Context Variable | Value                                                                |
|------------------|----------------------------------------------------------------------|
| target.name      | Name of the active db `target`                                       |
| target.dbname    | Database name specified in active target                             |
| target.host      | Host specified in active target                                      |
| target.schema    | Schema specified in active target                                    |
| target.type      | Database type (postgres / redshift / ...) specified in active target |
| target.user      | User specified in active target                                      |
| target.port      | Port specified in active target                                      |
| target.threads   | Number of threads specified in active target                         |
| run_started_at   | Timestamp when the run started (eg. 2017-01-01 01:23:45.678)         |
| invocation_id    | A UUID generated for this dbt run (useful for auditing)              |


## adapter

`adapter` makes available some internal dbt functions that are useful for implementing custom logic in your
dbt models. For example, you could write your own advanced incremental model using code like:

```sql
-- some_model.sql

select * from {{ref('raw_table')}}

{% if adapter.already_exists(this.schema, this.name) %}
  where id > (select max(id) from {{this}})
{% endif %}
```

Functions are documented below.

---

#### adapter.already_exists

__Args__:

 * `schema`: The schema to test
 * `table`: The relation to look for

Returns true if a relation named like `table` exists in schema `schema`, false otherwise.

__Example__:
```sql
select * from {{ref('raw_table')}}

{% if adapter.already_exists(this.schema, this.name) %}
  where id > (select max(id) from {{this}})
{% endif %}
```

([Source on Github](https://github.com/fishtown-analytics/dbt/blob/v0.8.0/dbt/wrapper.py#L165-L167))

---

#### adapter.get_columns_in_table

__Args__:

 * `schema_name`: The schema to test
 * `table_name`: The relation from which to select columns

Returns a list of [Columns](https://github.com/fishtown-analytics/dbt/blob/v0.8.0/dbt/schema.py#L37) in a table. Useful for writing `INSERT ... SELECT` queries.

__Example__:

```sql
{% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
{% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

insert into {{ this }} ({{ dest_cols_csv }}) (
  select {{ dest_cols_csv }}
  from {{ref('another_table')}}
);
```

([Source on Github](https://github.com/fishtown-analytics/dbt/blob/v0.8.0/dbt/wrapper.py#L169-L171))

---

#### adapter.get_missing_columns

__Args__:

 * `from_schema`: The schema for the `from_table`
 * `from_table`: The `from_table` to check for differences
 * `to_schema`: The schema for the `to_table`
 * `to_table`: The `to_table` to check for differences

Returns the set of [Columns](https://github.com/fishtown-analytics/dbt/blob/v0.8.0/dbt/schema.py#L37) that is the difference of the columns in the `from_table`
and the columns in the `to_table`, i.e. (`set(from_table.columns) - set(to_table.columns)`).
Useful for detecting new columns in a source table.

__Example__:

```sql
{% for col in adapter.get_missing_columns(this.schema, 'source_table', this.schema, this.name) %}
  alter table {{this}} add column "{{col.name}}" {{col.data_type}};
{% endfor %}
```

([Source on Github](https://github.com/fishtown-analytics/dbt/blob/v0.8.0/dbt/wrapper.py#L173-L177))

---

## Arbitrary configuration variables

Variables can be passed from your `dbt_project.yml` file into models during compilation.
These variables are useful for configuring packages to be useful in multiple environments.
To add a variable to a model, use the var() function:

```sql
-- some_model.sql

select * from events where event_type = '{{ var("event_type") }}'
```

If you try to run this model without supplying an `event_type` variable, you'll recieve
a compilation error that looks like this:

```
Encountered an error:
! Compilation error while compiling model package_name.some_model:
! Required var 'event_type' not found in config:
Vars supplied to package_name.some_model = {
}
```

To supply a variable to a given model, add one or more `vars` dictionaries to the `models`
config in your `dbt_project.yml`. These `vars` are in-scope for all models at or below
where they are defined, so place them where they make the most sense. Below are three different
placements of the `vars` dict, all of which will make the `some_model` model compile.

```yml
# dbt_project.yml

# 1) scoped at the model level
models:
    package_name:
        some_model:
            vars:
                event_type: activation

# 2) scoped at the package level
models:
    package_name:
        vars:
            event_type: activation
        some_model:

# 3) scoped globally
models:
    vars:
        event_type: activation
    package_name:
        some_model:
```
