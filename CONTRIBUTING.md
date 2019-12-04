# Getting started with dbt

## About this document

This document is a guide intended for folks interested in contributing to dbt. It is not intended as a guide for end users of dbt (though if it is helpful, that's great!) and it assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, python modules, filesystems, and so on. This guide also documents the process by which community-contributed Pull Requests can be incorporated into this repository. This guide assumes you are using macOS or Linux and are comfortable with the command line. If you get stuck while reading this guide, drop us a line in the #development channel on [slack](slack.getdbt.com).

## Contributing a change

dbt is Apache 2.0-licensed open source software. dbt is the software that it is today because community members like you have opened issues, provided feedback, and contributed to the knowledge loop for the entire communtiy. Whether you are a seasoned open source contributor or a first time committer, you are welcomed and encouraged to contribute code, documentation, ideas, or problem statements to this project.

### Defining the problem

If you have an idea for a new feature or if you've discovered a bug in dbt, the first step is to open an issue. Please check the list of [open issues](https://github.com/fishtown-analytics/dbt/issues) before creating a new one. If you find a relevant issue, please add a comment to the open issue instead of creating a new one. There are hundreds of open issues in this repository and it can be hard to know where to look for a relevant open issue. **The dbt maintainers are always happy to point contributors in the right direction**, so please err on the side of documenting your idea in a new issue if you are unsure where a problem statement belongs.

**Note:** All community-contributed Pull Requests _must_ be associated with an open issue. If you submit a Pull Request that does not pertain to an open issue, you will be asked to create an issue describing the problem before the Pull Request can be reviewed.

### Discussing the idea

After creating an issue, a dbt maintainer will follow up with you to explore your idea further and advise on how to implement the suggested changes. In many cases, community members will chime in with their own thoughts on the problem statement. If you as the issue creator are interested in submitting a Pull Request to address the issue, you should indicate this in the body of the issue. The dbt maintainers are _always_ happy to help contributors with the implementation of fixes and features, so please also indicate if there's anything you're unsure about or could use guidance around in the issue.

### Submitting a change

If the issue is appropriately well-scoped and describes a beneficial change to the dbt codebase, then anyone may submit a Pull Request to implement the functionality described in the issue (see the sections below on how to do this).

In some cases, the right resolution to an open issue might be tangential to the dbt codebase. The right path forward might be a documentation update or a change that can be made in user-space. In other cases, the issue might describe functionality that the dbt maintainers are unwilling or unable to incorporate into the dbt codebase. When it is determined that an open issue describes functionality that will not translate to a code change in the dbt repository, the issue will be tagged with the `wontfix` label (see below) and closed.

### Using issue labels

The dbt maintainers use labels to categorize open issues. Some labels indicate the databases impacted by the issue, while others describe the domain in the dbt codebase germane to the discussion. While most of these labels are self-explanatory (eg. `snowflake` or `bigquery`), there are others that are worth describing.

| tag | description |
| --- | ----------- |
| bug | This issue represents a defect or regression in dbt |
| enhancement | This issue represents net-new functionality in dbt |
| good first issue | This issue does not require deep knowledge of the dbt codebase to implement. This issue is appropriate for a first-time contributor to implement. |
| snoozed | This issue describes a good idea, but one which will probably not be addressed in a six-month time horizon. The dbt maintainers will revist these issues periodically and re-prioritize them accordingly. |
| triage | This is a new issue which has not yet been reviewed by a dbt maintainer. This label is removed when a maintainer reviews and responds to the issue. |
| stale | This is an old issue which has not recently been updated. Stale issues will periodically be closed by dbt maintainers, but they can be re-opened if the discussion is restarted. |
| wontfix | This issue does not require a code change in the dbt repository, or the maintainers are unwilling/unable to merge a Pull Request which implements the behavior described in the issue. |

### Signing the CLA

All contributors to dbt must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into the dbt codebase. If you are unable to sign the CLA, then the dbt maintainers will unfortunately be unable to merge your Pull Request.


## Getting the code

### Installing git

You will need `git` in order to download and modify the dbt source code. On macOS, the best way to download git is to just install Xcode.

### External contributors

If you are not a member of the `fishtown-analytics` GitHub organization, you can contribute to dbt by forking the dbt repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the dbt repository
2. clone your fork
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `fishtown-analytics/dbt` from your forked repository

### Core contributors

If you are a member of the `fishtown-analytics` GitHub organization, you will have push access to the dbt repo. Rather than 
forking dbt to make your changes, just clone the repository and push directly to a branch.


## Setting up an environment

To begin developing code in dbt, you should set up the following:

### virtualenv

We strongly recommend using virtual environments when developing code in dbt. We recommend creating this virtualenv
in the root of the dbt repository. To create a new virtualenv, run:
```
python3 -m venv env
source env/bin/activate
```

This will create and activate a new Python virtual environment.

### docker and docker-compose

Docker and docker-compose are both used in testing. For macOS, the easiest thing to do is to [download docker for mac](https://store.docker.com/editions/community/docker-ce-desktop-mac). You'll need to make an account. On Linux, you can use one of the packages [here](https://docs.docker.com/install/#server). We recommend installing from docker.com instead of from your package manager. On Linux you also have to install docker-compose separately, follow [these instructions](https://docs.docker.com/compose/install/#install-compose).


### Installing postgres locally (optional)

For testing, and later in the examples in this document, you may want to have `psql` available so you can poke around in the database and see what happened. We recommend that you use [homebrew](https://brew.sh/) for that on macOS, and your package manager on Linux. You can install any version of the postgres client that you'd like. On macOS, with homebrew setup, you can run:

```
brew install postgresql
```

## Running dbt in development

### Installation

First make sure that you set up your `virtualenv` as described in section _Setting up an environment_. Next, install dbt (and it's dependencies) with:

```
pip install -r editable_requirements.txt
```

When dbt is installed from source in this way, any changes you make to the dbt source code will be reflected immediately in your next `dbt` run.

### Running dbt

With your virtualenv activated, the `dbt` script should point back to the source code you've cloned on your machine. You can verify this by running `which dbt`. This command should show you a path to an executable in your virtualenv.

Configure your [profile](https://docs.getdbt.com/docs/configure-your-profile) as necessary to connect to your target databases. It may be a good idea to add a new profile pointing to a local postgres instance, or a specific test sandbox within your data warehouse if appropriate.

## Testing

Getting the dbt integration tests set up in your local environment will be very helpful as you start to make changes to your local version of dbt. The section that follows outlines some helpful tips for setting up the test environment.

### Tools

A short list of tools used in dbt testing that will be helpful to your understanding:

- [virtualenv](https://virtualenv.pypa.io/en/stable/) to manage dependencies
- [tox](https://tox.readthedocs.io/en/latest/) to manage virtualenvs across python versions
- [pytest](https://docs.pytest.org/en/latest/) to discover/run tests
- [make](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) - but don't worry too much, nobody _really_ understands how make works and our Makefile is super simple
- [flake8](https://gitlab.com/pycqa/flake8) for code linting
- [CircleCI](https://circleci.com/product/) and [Azure Pipelines](https://azure.microsoft.com/en-us/services/devops/pipelines/)

A deep understanding of these tools in not required to effectively contribute to dbt, but we recommend checking out the attached documentation if you're interested in learning more about them.


### Running tests via Docker

dbt's unit and integration tests run in Docker. Because dbt works with a number of different databases, you will need to supply credentials for one or more of these databases in your test environment. Most organizations don't have access to each of a BigQuery, Redshift, Snowflake, and Postgres database, so it's likely that you will be unable to run every integration test locally. Fortunately, Fishtown Analytics provides a CI environment with access to sandboxed Redshift, Snowflake, BigQuery, and Postgres databases. See the section on _Submitting a Pull Request_ below for more information on this CI setup.


#### Specifying your test credentials

dbt uses test credentials specified in a `test.env` file in the root of the repository. This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing against dbt. To create your `test.env` file, copy the provided sample file, then supply your relevant credentials:

```
cp test.env.sample test.env
atom test.env # supply your credentials
```

We recommend starting with dbt's Postgres tests. These tests cover most of the functionality in dbt, are the fastest to run, and are the easiest to set up. dbt's test suite runs Postgres in a Docker container, so no setup should be required to run these tests. If you additionally want to test Snowflake, Bigquery, or Redshift locally you'll need to get credentials and add them to the `test.env` file. 

#### Running tests

dbt's unit tests and Python linter can be run with:

```
make test-unit
```

To run the Postgres + Python 3.6 integration tests, you'll have to do one extra step of setting up the test database:

```
docker-compose up -d database
PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres bash test/setup_db.sh
```

To run a quick test for Python3 integration tests on Postgres, you can run:

```
make test-quick
```

To run tests for a specific database, invoke `tox` directly with the required flags:
```
# Run Postgres py36 tests
docker-compose run test tox -e integration-postgres-py36 -- -x

# Run Snowflake py36 tests
docker-compose run test tox -e integration-snowflake-py36 -- -x

# Run BigQuery py36 tests
docker-compose run test tox -e integration-bigquery-py36 -- -x

# Run Redshift py36 tests
docker-compose run test tox -e integration-redshift-py36 -- -x
```

See the `Makefile` contents for more some other examples of ways to run `tox`.

### Submitting a Pull Request

Fishtown Analytics provides a sandboxed Redshift, Snowflake, and BigQuery database for use in a CI environment.

When pull requests are submitted to the `fishtown-analytics/dbt` repo, GitHub will trigger automated tests in CircleCI and Azure Pipelines. If the PR submitter is a member of the `fishtown-analytics` GitHub organization, then the credentials for these databases will be automatically supplied as environment variables in the CI test suite.

**If the PR submitter is not a member of the `fishtown-analytics` organization, then these environment variables will not be automatically supplied in the CI environment**. Once a core maintainer has taken a look at the Pull Request, they will kick off the test suite with the required credentials.

Once your tests are passing and your PR has been reviewed, a dbt maintainer will merge your changes into the active development branch! And that's it! Happy developing :tada:
