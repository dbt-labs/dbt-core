# Building models #

Building data models is the core of using dbt. This section provides guidance on how to think about data models in dbt and how to go about building them.

## Everything is a select

The core concept of dbt data models is that everything is a `SELECT` statement. Using this approach, the SQL code within a given model defines the dataset, while dbt configuration defines what to do with it. This allows users to focus on writing analysis, not writing plumbing code. dbt is responsible for the plumbing: creating tables, inserting records, dropping tables, etc.

Here are some things that can be done when separating the analytic logic from the model configuration:

- With a single config change, one data model or an entire hierarchy of models can be flipped from views to materialized tables. dbt takes care of wrapping a model's `SELECT` statement in the appropriate `CREATE TABLE` or `CREATE VIEW` syntax.
- With two configuration changes, a model can be flipped from a materialized table that is rebuilt with every `dbt run` to a table that is built incrementally, inserting the most recent rows since the most recent `dbt run`. dbt will wrap the select into an `INSERT` statement and automatically generate the appropriate `WHERE` clause.
- With one config change, a model can be made ephemeral. Instead of being deployed into the database, ephemeral models are pulled into dependent models as common table expressions.

Because every model is a `SELECT`, these behaviors can all be configured very simply, allowing for flexibility in development workflow and production deployment.

### Start by building views

While you're building new dbt models, it's common to default to materialize these new models as views. Views deploy extremely quickly and they have few configuration options. Since model behavior is controlled by configuration, you can update the configuration for a specific model as your project needs evolve without modifying the model code.

In order to configure your project to default to views, you'll want to put the following into your `dbt_project.yml`:

```YAML
models:
  [your-project-name]:
    enabled: true
    materialized: view
```

## Using ref()

The most important function in dbt is `ref()`; it's impossible to build even moderately complex models without it. `ref()` is how you reference one model within another. This is a very common behavior, as typically models are built to be "stacked" on top of one another. Here is how this looks in practice:

```sql
--filename: model_a.sql

select *
from public.raw_data
```
```sql
--filename: model_b.sql

select *
from {{ref('model_a')}}
```

`ref()` is, under the hood, actually doing two important things. First, it is interpolating the schema into your model file to allow you to change your deployment schema via configuration. Second, it is using these references between models to automatically build the dependency graph. This will enable dbt to deploy models in the correct order when using `dbt run`.

Functions in dbt are wrapped in double brackets, so writing `ref('model_name')` must actually be done as `{{ref('model_name')}}`

## Example models

Here are some reference examples of dbt projects. Take a look at their code to get a sense of what a production dbt project can look like.

- [Quickbooks](https://github.com/fishtown-analytics/quickbooks)
- [Snowplow](https://github.com/fishtown-analytics/snowplow)
- [Stripe](https://github.com/fishtown-analytics/stripe)
- [Zendesk](https://github.com/fishtown-analytics/zendesk)
