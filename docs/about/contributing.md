# Contributing

## Code

We welcome PRs! We recommend that you log any feature requests as issues and discuss implementation approach with the team prior to getting to work. In order to get set up to develop, run the following from the root project directory to install a development version of dbt:

```bash
› python setup.py develop
```


## Docs

We welcome PRs with updated documentation! All documentation for dbt is written in markdown using [mkdocs](http://www.mkdocs.org/). Please follow installation instructions there to set up mkdocs on your local environment.

## Design Constraints

All contributions to dbt must adhere to the following design constraints:

- All data models are a single `SELECT` statement. All decisions about how the results of that statement are materialized in the database must be user-controlled via configuration.
- The target schema must always be able to be regenerated from scratch—i.e. if a user performs a `DROP SCHEMA [target] CASCADE` and then runs `dbt run --target [target]`, all data will be re-built exactly as before.

## Design Principles

When contributing to dbt, please keep the core project goal in mind:

**dbt (data build tool) is a productivity tool that helps analysts get more done and produce higher quality results.**

This goal has been carefully selected, and flows directly from the [viewpoint](about/viewpoint/).

### Why do we call dbt a “productivity tool”? Doesn't this minimize its impact?

This is a deliberate choice of words that forces us to remember what dbt actually is and what its goals are: *dbt is a user experience wrapper around an analytic database.* All design decisions should be made with the goal of creating a better workflow / user experience for analysts.

### Why are we focused on speed and quality as opposed to capability?

Most analytics tools that exist today were designed to maximize user capability. If an analyst wanted to build a line chart, the tool needed to make sure he/she could build that line chart. Exactly how that line chart was produced was less important.

This perspective made sense in the past. It used to be hard to make a line chart. Today it is easy: using matplotlib, ggplot2, Tableau, or the countless other charting tools creates functionally the same result. Today, the hard part is not making the line chart, but making the line chart fast, with accurate data, in a collaborative environment. While analysts today can create stunning visualizations, they struggle to produce accurate and timely data in a collaborative environment.

Analysts don’t need more new capabilities, they need a workflow that allows them to use the ones they have faster, with higher quality, and in teams.

### Why are we focused on analysts instead of data engineers?

Two reasons:

1. Analysts are closer to the business and the business users, and therefore have the information they need to actually build data models.
1. There are far more analysts than data engineers in the world. To truly solve this problem, we need to make the solution accessible for analysts.

### Why is dbt such a technical tool if its target users are analysts?

Most analysts today don't spend their time in text files and on the command line, but dbt forces the user to do both. This choice was made intentionally, and on three beliefs:

1. Analysts are already becoming more technical, and this trend will accelerate in coming years.
1. Working in this way allows dbt to hook into a much larger ecosystem of developer productivity tools like git, vim/emacs, etc. This ecosystem has a large part to play in the overall productivity gains to be had from dbt.
1. Core analytics workflows should not be locked away into a particular UI.
