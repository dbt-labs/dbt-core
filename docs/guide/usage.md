# Usage

The following commands are available from the command line to interact with dbt.

## Init

`dbt init [project_name]` performs several actions necessary to create a new dbt project:

- creates a new folder at `./[project_name]` that includes necessary dbt scaffolding, including directories and template configuration files, and
- creates necessary user-specific configuration files at `~/.dbt/` if these files do not already exist. [NOT YET IMPLEMENTED]

## Run

`dbt run` first compiles, and then runs, model files against the current `run-target` database. dbt connects to the run target and runs the relevant DDL and SQL required to materialize all data models using the configured materialization strategy. Models are run in the order defined by the dependency graph generated during compilation. Intelligent multi-threading is used to minimize execution time without violating dependencies.

Deploying new models frequently involves destroying prior versions of these models. In these cases, `dbt run` minimizes the amount of time in which a model is unavailable by first building each model with a temporary name, then dropping the existing model, then renaming the model to its correct name. The drop and rename happen within a single database transaction for database adapters that support transactions.

### Specify the models to run

Using `dbt run --models [model names]` will cause dbt to only deploy the models you list, along with their dependents. This can significantly decrease deployment time for large projects when only deploying a subset of models.

### Dry run

`dbt run --dry` checks the validity of data models by running them against the target database as views within a temporary namespace and subsequently dropping them. The user will be notified as to the success and failure of each model run.

`dbt run` is frequently destructive (it drops previous models at the same namespace). `dbt run --dry` is a useful way to verify that a run will complete in its entirety prior to a `dbt run`. Clean deployment signals that a subsequent `dbt run` in the same environment is likely to be successful.

Note that in its current version, `dbt run --dry` does not build any tables and therefore never executes `SELECT` statements on the underlying data. As such, there are modes in which `dbt run --dry` can succeed but `dbt run` can fail. Future versions will address this issue.

## Test

`dbt test` runs tests on data in deployed models. There are two types of tests:
- schema validations, declared in a `schema.yml` file.
- custom validations, written as SQL `SELECT` statements. [NOT YET IMPLEMENTED]

`dbt test` runs both types of test and reports the results to the console.

Model validation is discussed in more detail [here](model-validation/).

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
