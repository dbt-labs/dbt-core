# Configuring models #

There are a number of configuration options provided to control how dbt interacts with your models. Understanding these configuration options is core to controlling dbt's behavior and optimizing its usage.

## Supplying configuration values

There are multiple ways to supply model configuration values:

- as settings for an entire group of modules, applied at the directory level
- as settings for an individual model, specified within a given model.

All methods accept identical configuration options. dbt provides multiple configuration contexts in order to give model authors maximum control over their model behaviors. In all cases, configuration provided at a more detailed level overrides configuration provided at a more generic level.

Here is how these configuration options look in practice:

```YAML
# dbt_project.yml
# specify project- and directory-level configuration here.

models:
  [project_name]:
    [model-group]: # model groups can be arbitrarily nested and reflect the directory structure of your project.
      enabled: true
      materialized: view
      ...
```

```SQL
--[model_name].sql
--specify model-level configuration here.

-- python function syntax
{{
  config(
    materialized = "incremental",
    sql_where = "id > (select max(id) from {{this}})"
  )
}}

-- OR json syntax
{{
  config({
    "materialized" : "incremental",
    "sql_where" : "id > (select max(id) from {{this}})"
    })
}}
```

## Using enabled

This parameter does exactly what you might think. Setting `enabled` to `false` tells dbt not to compile and run the associated models. Be careful disabling large swaths of your project: if you disable models that are relied upon by enabled models in the dependency chain, compilation will fail.

Note that dbt does not actively delete models in your database that have been disabled. Instead, it simply leaves them out of future rounds of compilation and deployment. If you want to delete models from your schema, you will have to drop them by hand.

## Using materialized

The `materialized` option is provided like so:

```YAML
# dbt_project.yml

materialized: table # other values: view, incremental, ephemeral
```

Each of the four values passed to `materialized` significantly changes how dbt builds the associated models:

- `table` wraps the `SELECT` in a `CREATE TABLE AS...` statement. This is a good option for models that take a long time to execute, but requires the model to be re-built in order to get new data. Each time a `table` model is re-built, it is first dropped and then recreated.
- `view` wraps the `SELECT` in a `CREATE VIEW AS...` statement. This is a good option for models that do not take a long time to execute, as avoids the overhead involved in storing the model's data on disk.
- `incremental` allows dbt to insert or update records into a table since the last time that dbt was run. Incremental models are one of the most powerful features of dbt but require additional configuration; please see the section below for more information on how to configure incremental models.
- `ephemeral` prevents dbt from materializing the model directly into the database. Instead, dbt will interpolate the code from this model into dependent models as a common table expression. This allows the model author to write reusable logic that data consumers don't have access to `SELECT` directly from and thereby allows the analytic schema to act as the "public interface" that gets exposed to users.

### Configuring incremental models

Incremental models are a powerful feature in production dbt deployments. Frequently, certain raw data tables can have billions of rows, which makes performing frequent rebuilds of models dependent on these tables impractical. Incremental tables provide another option. The first time a model is deployed, the table is created and data is inserted. In subsequent runs this model will have new rows inserted and changed rows updated. (Technically, updates happen via deletes and then inserts.)

It's highly recommended to use incremental models rather than table models in production whenever the schema allows for it. This will minimize your model build times and minimize the use of database resources.

#### sql_where

`sql_where` identifies the rows that have been updated or added since the most recent run. For instance, in a clickstream table, you might apply the condition:

```SQL
WHERE [source].session_end_timestamp >= (select max(session_end_timestamp) from [model])
```

dbt applies this `WHERE` condition automatically, so it shouldn't be present in the model code: specify it in your model config as so `sql_where = "[condition]"`.

#### using {{this}}

`{{this}}` is a special variable that returns the schema and table name of the current model and is useful when defining a `sql_where` clause. The `sql_where` we wrote earlier would actually be written as such:

```SQL
WHERE session_end_timestamp >= (select max(session_end_timestamp) from {{this}})
```

See [context variables](context-variables/) for more information on `this`.

#### unique_key

`unique_key` is an optional parameter that specifies uniqueness on this table. Records matching this UK that are found in the table will be deleted before new records are inserted. Functionally, this allows for modification of existing rows in an incremental table. `unique_key` can be any valid SQL expression, including a single field, or a function. A common use case is concatenating multiple fields together to create a single unique key, as such: `user_id || session_index`.

## Database-specific configuration

In addition to the configuration parameters that apply to all database adapters, there are certain configuration options that apply only to specific databases. See the page on [database-specific optimizations](database-optimizations/).

## Hooks

dbt provides the ability to run arbitrary commands against the database before and after a model is run. These are known as pre- and post-model hooks and configured as such:

```YAML
models:
  project-name:
    pre-hook:       # custom SQL
    post-hook:      # custom SQL

```

Hooks are extremely powerful, allowing model authors to perform tasks such as inserting records into audit tables, executing `GRANT` statements, and running `VACUUM` commands, among others. To learn more about hooks and see examples, see [using hooks](using-hooks/).
