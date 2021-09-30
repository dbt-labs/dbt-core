# Contributing to `dbt`

1. [About this document](#about-this-document)
2. [Proposing a change](#proposing-a-change)
3. [Getting the code](#getting-the-code)
4. [Setting up an environment](#setting-up-an-environment)
5. [Running `dbt` in development](#running-dbt-in-development)
6. [Testing](#testing)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document

This document is a guide intended for folks interested in contributing to `dbt`. Below, we document the process by which members of the community should create issues and submit pull requests (PRs) in this repository. It is not intended as a guide for using `dbt`, and it assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, python modules, filesystems, and so on. This guide assumes you are using macOS or Linux and are comfortable with the command line.

If you're new to python development or contributing to open-source software, we encourage you to read this document from start to finish. If you get stuck, drop us a line in the `#dbt-core-development` channel on [slack](https://community.getdbt.com).

#### Adapters

If you have an issue or code change suggestion related to a specific database [adapter](https://docs.getdbt.com/docs/available-adapters); please refer to that supported databases seperate repo for those contributions.

### Signing the CLA

Please note that all contributors to `dbt` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into the `dbt` codebase. If you are unable to sign the CLA, then the `dbt` maintainers will unfortunately be unable to merge your Pull Request. You are, however, welcome to open issues and comment on existing ones.

## Proposing a change

`dbt` is Apache 2.0-licensed open source software. `dbt` is what it is today because community members like you have opened issues, provided feedback, and contributed to the knowledge loop for the entire communtiy. Whether you are a seasoned open source contributor or a first-time committer, we welcome and encourage you to contribute code, documentation, ideas, or problem statements to this project.

### Defining the problem

If you have an idea for a new feature or if you've discovered a bug in `dbt`, the first step is to open an issue. Please check the list of [open issues](https://github.com/dbt-labs/dbt/issues) before creating a new one. If you find a relevant issue, please add a comment to the open issue instead of creating a new one. There are hundreds of open issues in this repository and it can be hard to know where to look for a relevant open issue. **The `dbt` maintainers are always happy to point contributors in the right direction**, so please err on the side of documenting your idea in a new issue if you are unsure where a problem statement belongs.

> **Note:** All community-contributed Pull Requests _must_ be associated with an open issue. If you submit a Pull Request that does not pertain to an open issue, you will be asked to create an issue describing the problem before the Pull Request can be reviewed.

### Discussing the idea

After you open an issue, a `dbt` maintainer will follow up by commenting on your issue (usually within 1-3 days) to explore your idea further and advise on how to implement the suggested changes. In many cases, community members will chime in with their own thoughts on the problem statement. If you as the issue creator are interested in submitting a Pull Request to address the issue, you should indicate this in the body of the issue. The `dbt` maintainers are _always_ happy to help contributors with the implementation of fixes and features, so please also indicate if there's anything you're unsure about or could use guidance around in the issue.

### Submitting a change

If an issue is appropriately well scoped and describes a beneficial change to the `dbt` codebase, then anyone may submit a Pull Request to implement the functionality described in the issue. See the sections below on how to do this.

The `dbt` maintainers will add a `good first issue` label if an issue is suitable for a first-time contributor. This label often means that the required code change is small, limited to one database adapter, or a net-new addition that does not impact existing functionality. You can see the list of currently open issues on the [Contribute](https://github.com/dbt-labs/dbt/contribute) page.

Here's a good workflow:
- Comment on the open issue, expressing your interest in contributing the required code change
- Outline your planned implementation. If you want help getting started, ask!
- Follow the steps outlined below to develop locally. Once you have opened a PR, one of the `dbt` maintainers will work with you to review your code.
- Add a test! Tests are crucial for both fixes and new features alike. We want to make sure that code works as intended, and that it avoids any bugs  previously encountered. Currently, the best resource for understanding `dbt`'s [unit](test/unit) and [integration](test/integration) tests is the tests themselves. One of the maintainers can help by pointing out relevant examples.

In some cases, the right resolution to an open issue might be tangential to the `dbt` codebase. The right path forward might be a documentation update or a change that can be made in user-space. In other cases, the issue might describe functionality that the `dbt` maintainers are unwilling or unable to incorporate into the `dbt` codebase. When it is determined that an open issue describes functionality that will not translate to a code change in the `dbt` repository, the issue will be tagged with the `wontfix` label (see below) and closed.

### Using issue labels

The `dbt` maintainers use labels to categorize open issues. Some labels indicate the databases impacted by the issue, while others describe the domain in the `dbt` codebase germane to the discussion. While most of these labels are self-explanatory (eg. `snowflake` or `bigquery`), there are others that are worth describing.

| tag | description |
| --- | ----------- |
| [triage](https://github.com/dbt-labs/dbt/labels/triage) | This is a new issue which has not yet been reviewed by a `dbt` maintainer. This label is removed when a maintainer reviews and responds to the issue. |
| [bug](https://github.com/dbt-labs/dbt/labels/bug) | This issue represents a defect or regression in `dbt` |
| [enhancement](https://github.com/dbt-labs/dbt/labels/enhancement) | This issue represents net-new functionality in `dbt` |
| [good first issue](https://github.com/dbt-labs/dbt/labels/good%20first%20issue) | This issue does not require deep knowledge of the `dbt` codebase to implement. This issue is appropriate for a first-time contributor. |
| [help wanted](https://github.com/dbt-labs/dbt/labels/help%20wanted) / [discussion](https://github.com/dbt-labs/dbt/labels/discussion) | Conversation around this issue in ongoing, and there isn't yet a clear path forward. Input from community members is most welcome. |
| [duplicate](https://github.com/dbt-labs/dbt/issues/duplicate) | This issue is functionally identical to another open issue. The `dbt` maintainers will close this issue and encourage community members to focus conversation on the other one. |
| [snoozed](https://github.com/dbt-labs/dbt/labels/snoozed) | This issue describes a good idea, but one which will probably not be addressed in a six-month time horizon. The `dbt` maintainers will revist these issues periodically and re-prioritize them accordingly. |
| [stale](https://github.com/dbt-labs/dbt/labels/stale) | This is an old issue which has not recently been updated. Stale issues will periodically be closed by `dbt` maintainers, but they can be re-opened if the discussion is restarted. |
| [wontfix](https://github.com/dbt-labs/dbt/labels/wontfix) | This issue does not require a code change in the `dbt` repository, or the maintainers are unwilling/unable to merge a Pull Request which implements the behavior described in the issue. |

#### Branching Strategy

`dbt` has three types of branches:

- **Trunks** are where active development of the next release takes place. There is one trunk named `develop` at the time of writing this, and will be the default branch of the repository.
- **Release Branches** track a specific, not yet complete release of `dbt`. Each minor version release has a corresponding release branch. For example, the `0.11.x` series of releases has a branch called `0.11.latest`. This allows us to release new patch versions under `0.11` without necessarily needing to pull them into the latest version of `dbt`.
- **Feature Branches** track individual features and fixes. On completion they should be merged into the trunk branch or a specific release branch.

## Getting the code

### Installing git

You will need `git` in order to download and modify the `dbt` source code. On macOS, the best way to download git is to just install [Xcode](https://developer.apple.com/support/xcode/).

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt` by forking the `dbt` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `dbt-labs/dbt` from your forked repository

### Core contributors

If you are a member of the `dbt-labs` GitHub organization, you will have push access to the `dbt` repo. Rather than forking `dbt` to make your changes, just clone the repository, check out a new branch, and push directly to that branch.

## Setting up an environment

There are some tools that will be helpful to you in developing locally. While this is the list relevant for `dbt` development, many of these tools are used commonly across open-source python projects.

### Tools

A short list of tools used in `dbt` testing that will be helpful to your understanding:

- [`tox`](https://tox.readthedocs.io/en/latest/) to manage virtualenvs across python versions. We currently target the latest patch releases for Python 3.6, Python 3.7, Python 3.8, and Python 3.9
- [`pytest`](https://docs.pytest.org/en/latest/) to discover/run tests
- [`make`](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) - but don't worry too much, nobody _really_ understands how make works and our Makefile is super simple
- [`flake8`](https://flake8.pycqa.org/en/latest/) for code linting
- [`mypy`](https://mypy.readthedocs.io/en/stable/) for static type checking
- [Github Actions](https://github.com/features/actions)

A deep understanding of these tools in not required to effectively contribute to `dbt`, but we recommend checking out the attached documentation if you're interested in learning more about them.

#### virtual environments

We strongly recommend using virtual environments when developing code in `dbt`. We recommend creating this virtualenv
in the root of the `dbt` repository. To create a new virtualenv, run:
```sh
python3 -m venv env
source env/bin/activate
```

This will create and activate a new Python virtual environment.

#### docker and docker-compose

Docker and docker-compose are both used in testing. Specific instructions for you OS can be found [here](https://docs.docker.com/get-docker/).


#### postgres (optional)

For testing, and later in the examples in this document, you may want to have `psql` available so you can poke around in the database and see what happened. We recommend that you use [homebrew](https://brew.sh/) for that on macOS, and your package manager on Linux. You can install any version of the postgres client that you'd like. On macOS, with homebrew setup, you can run:

```sh
brew install postgresql
```

## Running `dbt` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](#setting-up-an-environment).  Also ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt` (and its dependencies) with:

```sh
make dev
# or
pip install -r dev-requirements.txt -r editable-requirements.txt
```

When `dbt` is installed this way, any changes you make to the `dbt` source code will be reflected immediately in your next `dbt` run.

### Running `dbt`

With your virtualenv activated, the `dbt` script should point back to the source code you've cloned on your machine. You can verify this by running `which dbt`. This command should show you a path to an executable in your virtualenv.

Configure your [profile](https://docs.getdbt.com/docs/configure-your-profile) as necessary to connect to your target databases. It may be a good idea to add a new profile pointing to a local postgres instance, or a specific test sandbox within your data warehouse if appropriate.

## Testing

Getting the `dbt` integration tests set up in your local environment will be very helpful as you start to make changes to your local version of `dbt`. The section that follows outlines some helpful tips for setting up the test environment.

Although `dbt` works with a number of different databases, you won't need to supply credentials for every one of these databases in your test environment. Instead you can test all dbt-core code changes with Python and Postgres.

### Initial setup

We recommend starting with `dbt`'s Postgres tests. These tests cover most of the functionality in `dbt`, are the fastest to run, and are the easiest to set up. To run the Postgres integration tests, you'll have to do one extra step of setting up the test database:

```sh
make setup-db
```
or, alternatively:
```sh
docker-compose up -d database
PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres bash test/setup_db.sh
```

Note that you may need to run the previous command twice as it does not currently wait for the database to be running before attempting to run commands against it.  This will be fixed with [#3876](https://github.com/dbt-labs/dbt/issues/3876).

`dbt` uses test credentials specified in a `test.env` file in the root of the repository for non-Postgres databases. This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing against `dbt`. To create your `test.env` file, copy the provided sample file, then supply your relevant credentials. This step is only required to use non-Postgres databases.

```
cp test.env.sample test.env
$EDITOR test.env
```

> In general, it's most important to have successful unit and Postgres tests. Once you open a PR, `dbt` will automatically run integration tests for the other three core database adapters. Of course, if you are a BigQuery user, contributing a BigQuery-only feature, it's important to run BigQuery tests as well.

### Test commands

There are a few methods for running tests locally.

#### Makefile

There are multiple targets in the Makefile to run common test suites and code
checks, most notably:

```sh
# Runs unit tests with py38 and code checks in parallel.
make test
# Runs postgres integration tests with py38 in "fail fast" mode.
make integration
```
> These make targets assume you have a recent version of [`tox`](https://tox.readthedocs.io/en/latest/) installed locally,
> unless you use choose a Docker container to run tests. Run `make help` for more info.

Check out the other targets in the Makefile to see other commonly used test
suites.

#### `tox`

[`tox`](https://tox.readthedocs.io/en/latest/) takes care of managing virtualenvs and install dependencies in order to run
tests. You can also run tests in parallel, for example, you can run unit tests
for Python 3.6, Python 3.7, Python 3.8, `flake8` checks, and `mypy` checks in
parallel with `tox -p`. Also, you can run unit tests for specific python versions
with `tox -e py36`. The configuration for these tests in located in `tox.ini`.

#### `pytest`

Finally, you can also run a specific test or group of tests using [`pytest`](https://docs.pytest.org/en/latest/) directly. With a virtualenv
active and dev dependencies installed you can do things like:
```sh
# run specific postgres integration tests
python -m pytest -m profile_postgres test/integration/001_simple_copy_test
# run all unit tests in a file
python -m pytest test/unit/test_graph.py
# run a specific unit test
python -m pytest test/unit/test_graph.py::GraphTest::test__dependency_list
```
> [Here](https://docs.pytest.org/en/reorganize-docs/new-docs/user/commandlineuseful.html)
> is a list of useful command-line options for `pytest` to use while developing.
## Submitting a Pull Request

dbt Labs provides a CI environment to test changes to specific adapters, and periodic maintenance checks of `dbt-core` through Github Actions. For example, if you submit a pull request to the `dbt-redshift` repo, GitHub will trigger automated code checks and tests against Redshift.

A `dbt` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
