# Context variables

dbt makes available certain variables for developers to use to control a model's behavior. These variables may be defined within a certain model scope within the model hierarchy or may be global.

## this

`this` returns the schema and table name of the current model. This is useful in any context in which you need to write code that references the current model, for example when defining a `sql_where` clause for an incremental model and for writing pre- and post-model hooks that operate on the model in some way. Developers have options for how to use `this`:

```Python
this            #returns "schema"."table"
this.schema     #returns schema
this.table      #returns the name of the table that is being operated on in the database transaction.
                #sometimes dbt creates temporary tables; if that is true in this instance,
                #the temporary table name will be returned.
this.name       #returns the logical table name (ignores temporary tables)
```

In most cases, the correct choice will be to simply use `this`, although at times the other derivatives will be useful. Developers are left to choose what is most appropriate.

Here's an example of how to use `this` to write a `sql_where` clause that only inserts the new records since the most recent timestamp found in the table:

```YAML
sessions:
  materialized: incremental
  sql_where: "session_end_tstamp > (select max(session_end_tstamp) from {{this}})"
```


Here's an example of how to use `this` to create a pre-hook that insert records into an audit table for every model before and after it is built.

```YAML
models:
  project-name:
    pre-hook: "insert into _dbt.audit (event_name, event_timestamp, event_schema, event_model) values ( 'starting model deployment', getdate(), '{{this.schema}}', '{{this.name}}')"
```

## compiled_at

`compiled_at` returns the compilation time. This can be useful when auditing the results of a run.
