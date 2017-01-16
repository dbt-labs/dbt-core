# Usage

The following commands are available from the command line to interact with dbt.

## Init

`dbt init [project_name]` performs several actions necessary to create a new dbt project:

- creates a new folder at `./[project_name]`
- generates directories and sample files necessary to get started with dbt

## Run

`dbt run` first compiles, and then runs, model files against the current `target` database. dbt connects to the target database and runs the relevant SQL required to materialize all data models using the specified materialization strategies. Models are run in the order defined by the dependency graph generated during compilation. Intelligent multi-threading is used to minimize execution time without violating dependencies.

Deploying new models frequently involves destroying prior versions of these models. In these cases, `dbt run` minimizes the amount of time in which a model is unavailable by first building each model with a temporary name, then dropping the existing model, then renaming the model to its correct name. The drop and rename happen within a single database transaction for database adapters that support transactions.

### Specify the models to run

Using `dbt run --models [model names]` will cause dbt to only deploy the models you list, along with their dependents. This can significantly decrease deployment time for large projects when only deploying a subset of models.


### Run dbt non-destructively

If you provide the `--non-destructive` argument to `dbt run`, dbt will minimize the amount of destructive changes it runs against your database. Specifically, dbt
will:

 1. Ignore models materialized as `views`
 2. Truncate tables and re-insert data instead of dropping and re-creating these tables

This flag is useful for recurring jobs which only need to update table models and incremental models. DBT will _not_ create, drop, or modify views if the `--non-destructive` flag is provided.

```bash
dbt run --non-destructive
```

### Refresh incremental models

If you provide the `--full-refresh` argument to `dbt run`, dbt will treat incremental models as table models. This is useful when

1. The schema of an incremental model changes and you need to recreate it
2. You want to reprocess the entirety of the incremental model because of new logic in the model code

```bash
dbt run --full-refresh
```

## Test

`dbt test` runs tests on data in deployed models. There are two types of tests:

- schema validations, declared in a `schema.yml` file.
- custom data tests, written as SQL `SELECT` statements.

`dbt test` runs both types of test and reports the results to the console.

Model validation is discussed in more detail [here](testing/).

## Archive

`dbt archive` records snapshots of specified tables so that you can analyze how tables change over time. See [here](archival/) for more information.

## Dependencies

`dbt deps` pulls the most recent version of the dependencies listed in your `dbt_project.yml` from git. See [here](package-management/) for more information on dependencies.

## Compile

`dbt compile` generates runnable SQL from model files. All templating is completed and the dependency graph is built. Resulting SQL files are stored in the `target` directory.

Note that `dbt run` already includes this compilation step. As such, it is not necessary to use `dbt compile` before `dbt run`. Use `dbt compile` to compile SQL models without running them against your database.

## Debug

`dbt debug` is a utility function to show debug information.

## Clean

`dbt clean` is a utility function that deletes all compiled files in the `target` directory.

## Version

`dbt --version` is a utility function to check the version of your installed dbt client.
