# Best practices #

This page contains the collective wisdom of early users of dbt on how to best use it in your analytics work.

## Limit dependencies on raw data

Your project will depend on raw data stored in your database. We recommend making what we call "base models" to minimize the dependencies on raw data tables. In this convention, base models can have the following responsibilities:

- Select only the fields that are relevant for current analytics to limit complexity. More fields can always be added later.
- Perform any needed type conversion.
- Perform field aliasing to rationalize field names into a standard format used within the project.
- **Act as the sole access point to a given raw data table.**

In this convention, all subsequent data models are built on top of base models rather than on top of raw data—only base models are allowed to select from raw data tables. This ensures both that all of the transformations within the base model will be applied to all uses of this data and that if the source data table moves (or is located in a different schema or table in a different environment) it can be renamed in a single place.

For a simple example of a base model, check out this [Quickbooks base model](https://github.com/fishtown-analytics/quickbooks/blob/master/models/base/quickbooks_bills.sql).

## Managing multiple environments

dbt supports multiple `target`s within a given project within `~/.dbt/profiles.yml`. Users can configure a default `target` and can override this setting with the `--target` flag passed to `dbt run`. We recommend setting your default `target` to your development environment, and then switch to your production `target` specifically to deploy to production.

Using `target` to manage multiple environments gives you the flexibility set up your environments how you choose. Commonly, environments are managed by schemas within the same database: all test models are deployed to a schema called `dbt_[username]` and production models are deployed to a schema called `analytics`. An ideal setup would have production and test databases completely separate. Either way, we highly recommend maintaining multiple environments and managing deployments with `target`.

## Source control workflows

We believe that all dbt projects should be managed via source control. We use git for all of our source control, and use branching and pull requests to keep the master branch the sole source of organizational truth.

## Using dbt interactively

When your project gets large enough, `dbt run` can take a while. dbt provides three primary ways to address this so that you can deploy changes to your database quickly:

1. Use views instead of tables to the greatest extent possible in development. Views typically deploy much faster than tables, and in development it's often not critical that subsequent analytic queries run as fast as possible. It's easy to change this setting later and it will have no impact on your business logic.
1. Use `dbt_project.yml` to disable portions of your project that you're not currently working on. If you have multiple modules within a given project, turn off the ones that you're not currently working on so that those models don't deploy with every `dbt run`.
1. Pass the `--models` flag to `dbt run`. This flag causes dbt to only deploy the models you specify and their dependents. If you're working on a particular model, this can make a very significant difference in your deployment time in large projects.
