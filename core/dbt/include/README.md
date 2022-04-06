# Include Module

The Include module is reponsible for housing default macro definitions, starter project scaffold, and the html file used to generate the docs page.

# Directories

## `global_project`
Defines the default implementations of jinja2 macros for `dbt-core` which can be overwritten in each adapter repo to work more in line with those adapter plugins. To view adapter specific jinja2 changes please check the relevant adapter repo [`adapter.sql` ](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/include/bigquery/macros/adapters.sql) file in the `include` directory or in the [`impl.py`](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/adapters/bigquery/impl.py) file for some ex. BigQuery (truncate_relation).

## `starter_project`
Produces the default project after running the `dbt init` command for the CLI. `dbt-cloud` initializes the project by using [dbt-starter-project](https://github.com/dbt-labs/dbt-starter-project).


# Files
 - `index.html` a file generated from [dbt-docs](https://github.com/dbt-labs/dbt-docs) prior to new releases and replaced in the `dbt-core` directory. It is used to generate the docs page after using the `generate docs` command in dbt.
