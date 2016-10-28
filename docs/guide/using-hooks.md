# Using hooks

dbt provides the ability to run arbitrary commands against the database before and after a model is run. These are known as pre- and post-model hooks and configured as such:

```YAML
models:
  project-name:
    pre-hook:       # custom SQL
    post-hook:      # custom SQL

```

Hooks are extremely powerful, allowing model authors to perform tasks such as inserting records into audit tables, executing `GRANT` statements, and running `VACUUM` commands, among others.

`pre-hook` and `post-hook` configuration options apply, like all configuration options, to the scope in which they are defined in the model hierarchy. The example above defines them at the level of the entire project; they can also be specified within a given directory of that project or for an individual model.

Multiple instances of `pre-hook` and `post-hook` may be defined. In this case, dbt will run each pre-hook and post-hook using the following ordering:

- Hooks from dependent packages will be run before hooks in the active package.
- Hooks defined within the model itself will be run before hooks defined in `dbt_project.yml`.
- Hooks within a given context will be run in the order in which they are defined.

## Using hooks to create an audit table

Here's an example of how to use hooks to insert records into an audit table for every model before and after it is built.

```YAML
models:
  project-name:
    pre-hook: "insert into _dbt.audit (event_name, event_timestamp, event_schema, event_model) values ( 'starting model deployment', getdate(), '{{this.schema}}', '{{this.name}}')"
    post-hook: "insert into _dbt.audit (event_name, event_timestamp, event_schema, event_model) values ( 'completed model deployment', getdate(), '{{this.schema}}', '{{this.name}}')"
```

## Using hooks to vacuum

Incremental models can be configured to both insert new records and update existing records. In practice, updating existing records functions as a delete and insert. In Amazon Redshift, this will, over time, result in a poorly-optimized table. To address this, developers can include a post-hook to perform a vacuum command. For example:

```SQL
--[model_name].sql

{{
  config(
    post-hook = "vacuum delete only {{this}} to 100 percent",
  )
}}
```

It's possible to use this strategy to perform other database maintenance tasks on dbt models such as `analyze`.

## Using models to grant

Because many dbt model configurations result in dbt dropping and then recreating that model with every `dbt run`, it can be a challenge in certain environments to ensure that appropriate users have access to `SELECT` from appropriate models. Post-model hooks address this: the following configuration will run a `GRANT` statement for every model in the project:

```YAML
models:
  project-name:
    post-hook: "grant select on {{this}} to looker_user"
```
