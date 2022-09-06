# Include Module

The Include module is responsible for housing default macro definitions, starter project scaffold, and the html file used to generate the docs page.

# Directories

## `global_project`
Defines the default implementations of jinja2 macros for `dbt-core` which can be overwritten in each adapter repo to work more in line with those adapter plugins. To view adapter specific jinja2 changes please check the relevant adapter repo [`adapter.sql` ](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/include/bigquery/macros/adapters.sql) file in the `include` directory or in the [`impl.py`](https://github.com/dbt-labs/dbt-bigquery/blob/main/dbt/adapters/bigquery/impl.py) file for some ex. BigQuery (truncate_relation).

These directories contain the macros which wrap model code in DDL/DML. That code, in turn, “materializes” as a fully fledged database object in a warehouse pointed to by the current `target`. Here’re the major steps of this process:
    1. the SQL `select` query is compiled. In its current state, it could be run directly through a query editor to retrieve a dataset that will match 1:1 the database object materialized by `dbt`.
2. `dbt` embeds this SQL query into a materialization macro.
3. `dbt` executes this materialization macro which unravels a table into a DML/DDL statement that creates/truncate-and-replenishes/replaces a relation (e.g. view, table) in the `target`.

Note: `dbt compile` will not progress past step 1. It places the code produced in `target/compiled`.

### adapter.dispatch
Packages (e.g. `include` directories of adapters, any [hub](https://hub.getdbt.com/)-hosted package) can be interpreted as namespaces of functions a.k.a macros. In `dbt`'s macrospace, we take advantage of the multiple dispatch programming language concept. In short, multiple dispatch supports dynamic searching for a function across several namespaces—usually in a manually specified manner/order.

Adapters can have their own implementation of the same macro X. For example, a macro executed by `dbt-redshift` may need a specific implementation different from `dbt-snowflake`'s macro. We use multiple dispatch via `adapter.dispatch`, a Jinja function, which enables polymorphic macro invocations. The chosen implementation is selected according to what the `adapter` object is set to at runtime (it could be for redshift, postgres, and so on).

For more on this object, check out the dbt docs [here](https://docs.getdbt.com/reference/dbt-jinja-functions/adapter).

## `starter_project`
Produces the default project after running the `dbt init` command for the CLI. `dbt-cloud` initializes the project by using [dbt-starter-project](https://github.com/dbt-labs/dbt-starter-project).


# Files
 - `index.html` a file generated from [dbt-docs](https://github.com/dbt-labs/dbt-docs) prior to new releases and replaced in the `dbt-core` directory. It is used to generate the docs page after using the `generate docs` command in dbt.

# dbt and database adapter python package interop

Let’s say we have a fictional python app named `dbt-core` with this structure

```
dbt
├── adapters
│   └── base.py
├── cli.py
└── main.py
```

`pip install dbt-core` will install this application in my python environment, maintaining the same structure. Note that `dbt.adapters` only contains a `base.py`. In this example, we can assume that base.py includes an abstract class for creating connections. Let’s say we wanted to create an postgres adapter that this app could use, and can be installed independently. We can create a python package with the following structure called `dbt-postgres`
```
dbt
└── adapters
    └── postgres
        └── impl.py
```

`pip install dbt-postgres` will install this package in the python environment, maintaining the same structure again. Let’s say `impl.py` imports `dbt.adapters.base` and implements a concrete class inheriting from the abstract class in `base.py` from the `dbt-core` package. Since our top level package is named the same in both packages, `pip` will put this in the same place. We end up with this installed in our python environment.

```
dbt
├── adapters
│   ├── base.py
│   └── postgres
│       └── impl.py
├── cli.py
└── main.py
```

`dbt.adapters` now has a postgres module that dbt can easily find and call directly. dbt and its adapters follows the same type of file structure convention. This is the magic that allows you to import `dbt.*` in database adapters, and using a factory pattern in dbt-core, we can create instances of concrete classes defined in the database adapter packages (for creating connections, defining database configuration, defining credentials, etc.)
