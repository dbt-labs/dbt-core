# Overview #

## What is dbt?

dbt (data build tool) is a productivity tool that helps analysts get more done and produce higher quality results.

Analysts commonly spend 50-80% of their time modeling raw data—cleaning, reshaping, and applying fundamental business logic to it. dbt empowers analysts to do this work better and faster.

dbt's primary interface is its CLI. Using dbt is a combination of editing code in a text editor and running that code using dbt from the command line using `dbt [command] [options]`.

## How does dbt work?

dbt has two core workflows: building data models and testing data models. (We call any transformed view of raw data a data model.)

To create a data model, an analyst simply writes a SQL `SELECT` statement. dbt then takes that statement and builds it in the database, materializing it as either a view or a table. This model can then be queried by other models or by other analytics tools.

To test a data model, an analyst asserts something to be true about the underlying data. For example, an analyst can assert that a certain field should never be null, should always hold unique values, or should always map to a field in another table. Analysts can also write assertions that express much more customized logic, such as “debits and credits should always be equal within a given journal entry”. dbt then tests all assertions against the database and returns success or failure responses.

## Does dbt really help me get more done?

One dbt user has this to say: *“At this point when I have a new question, I can answer it 10-100x faster than I could before.”*  Here’s how:

- dbt allows analysts avoid writing boilerplate DML and DDL: managing transactions, dropping tables, and managing schema changes. All business logic is expressed in SQL `SELECT` statements, and dbt takes care of materialization.
- dbt creates leverage. Instead of starting at the raw data with every analysis, analysts instead build up reusable data models that can be referenced in subsequent work.
- dbt includes optimizations for data model materialization, allowing analysts to dramatically reduce the time their queries take to run.

There are many other optimizations in the dbt to help you work quickly: macros, hooks, and package management are all accelerators.

## Does dbt really help me produce more reliable analysis?

It does. Here’s how:

- Writing SQL frequently involves a lot of copy-paste, which leads to errors when logic changes. With dbt, analysts don’t need to copy-paste. Instead, they build reusable data models that then get pulled into subsequent models and analysis. Change a model once and everything that’s build on it reflects that change.
- dbt allows subject matter experts to publish the canonical version of a particular data model, encapsulating all complex business logic. All analysis on top of this model will incorporate the same business logic without needing to understand it.
- dbt plays nicely with source control. Using dbt, analysts can use mature source control processes like branching, pull requests, and code reviews.
- dbt makes it easy and fast to write functional tests on the underlying data. Many analytic errors are caused by edge cases in the data: testing helps analysts find and handle those edge cases.

## Why SQL?

While there are a large number of great languages for manipulating data, we’ve chosen SQL as the primary data transformation language at the heart of dbt. There are two reasons for this:

1. SQL is a very widely-known language for working with data. Using SQL gives the largest-possible group of users access.
1. Modern analytic databases are extremely performant and have sophisticated optimizers. Writing data transformations in SQL allows users to describe transformations on their data but leave the execution plan to the underlying database technology. In practice, this provides excellent results with far less work on the part of the author.

## What databases does dbt currently support?
Currently, dbt supports PostgreSQL and Amazon Redshift. We anticipate building support for additional databases in the future.

## How do I get started?

dbt is open source and completely free to download and use. See our [setup instructions](guide/setup/) for more.
