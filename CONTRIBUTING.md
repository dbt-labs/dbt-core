# Getting started with dbt

## About this document

This is intended as a developer's guide to modifying and using dbt. It is not intended as a guide for end users of dbt (though if it is helpful, that's great!) and assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, module/filesystem layouts, etc. It also assumes you are using macOS or Linux and are comfortable with the command line.

## Setting up an environment

Before you can develop dbt effectively, you should set up the following:

### pyenv

We strongly recommend setting up [pyenv](https://github.com/pyenv/pyenv) and its [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin. This setup will make it much easier for you to manage multiple Python projects in the medium to long term.

There is more documentation in each of those links on how to get set up, but the commands you'll need to run will be:
```
brew install pyenv
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.bash_profile
exec "$SHELL"
brew install pyenv-virtualenv
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

### python

By default, `pyenv` has only one python version installed and it's the `system` python - the one that comes with your OS. You don't want that. Instead, use `pyenv install 3.6.5` to install a more recent version. dbt supports up to Python 3.6 at the time of writing (and will soon support Python 3.7)

If you get the following error:
```
import pip
zipimport.ZipImportError: can't decompress data; zlib not available
make: *** [install] Error 1
```

You can solve it by running `brew install zlib`, then try `pyenv install 3.6.5` again.

To get a full (very long!) list of versions available, you can do `pyenv install -l` and look for the versions defined by numbers alone - the others are variants of Python and outside the scope of this document.

### docker and docker-compose

Docker and docker-compose are both used in testing. For macOS, the easiest thing to do is to go [here](https://store.docker.com/editions/community/docker-ce-desktop-mac) and download it. You'll need to make an account. On Linux, if you can use one of the packages [here](https://docs.docker.com/install/#server). We recommend installing from docker.com instead of from your package manager. On Linux you also have to install docker-compose separately, follow [these instructions](https://docs.docker.com/compose/install/#install-compose).

### git

You will also need `git` in order to get dbt and contribute code. On macOS, the best way to get that is to just install Xcode.

### GitHub

You will need a GitHub account fully configured with SSH to contribute to dbt. GitHub has [an excellent guide on how to set up SSH](https://help.github.com/articles/connecting-to-github-with-ssh/) -- we strongly recommend you follow their guide if you are unfamiliar with SSH.

### Getting dbt

Now clone dbt to wherever you'd like. For example:

```
mkdir -p ~/git/
cd ~/git
git clone git@github.com:fishtown-analytics/dbt.git
```

But it really does not matter where you put it as long as you remember it.


### Installing postgres locally

For testing, and later in the examples in this document, you may want to have `psql` available so you can poke around in the database and see what happened. We recommend that you use [homebrew](https://brew.sh/) for that on macOS, and your package manager on Linux. You can install any version of the postgres client that you'd like. So on macOS, with homebrew setup:

```
brew install postgresql
```

### Setting up your virtualenv

Set up a fresh virtualenv with pyenv-virtualenv for dbt:

```
pyenv virtualenv 3.6.5 dbt36
cd ~/git/dbt
pyenv local dbt36
pyenv activate
```

This makes a new virtualenv based on python 3.6.5 named `dbt36`, and tells pyenv that when you're in the `dbt` directory it should automatically use that virtualenv.

## Testing

Getting the dbt integration tests set up in your local environment will be very helpful as you start to make changes to your local version of dbt. The section that follows outlines some helpful tips for setting up the test environment.

### Tools

A short list of tools used in dbt testing that will be helpful to your understanding:

- [virtualenv](https://virtualenv.pypa.io/en/stable/) to manage dependencies and stuff
- [tox](https://tox.readthedocs.io/en/latest/) to manage virtualenvs across python versions
- [nosetests](http://nose.readthedocs.io/en/latest) to discover/run tests
- [make](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) - but don't worry too much, nobody _really_ understands how make works and our Makefile is super simple
- [flake8](https://gitlab.com/pycqa/flake8) for code linting
- [CircleCI](https://circleci.com/product/) and [Azure Pipelines](https://azure.microsoft.com/en-us/services/devops/pipelines/)

If you're unfamiliar with any or all of these, that's fine! You really do not have to have a deep understanding of any of these to get by.

Our test environment goes like this:

    - CircleCI and Appveyor run `tox`
    - `make test` runs `docker-compose`
    - `docker-compose` runs `tox`
    - `tox` sets up virtualenvs for each distinct set of tests and runs `nosetests`
    - `nosetests` finds all the appropriate tests and runs them

### Running tests via Docker

The basics should work with basically no further setup. In the terminal, `cd` to the directory where you cloned dbt. So, for example, if you cloned dbt to `~/git/dbt`:

```
cd ~/git/dbt
```

Then you'll want to make a test.env file. Fortunately, there's a sample which is fine for our purposes:

```
cp test.env.sample test.env
```

If you want to test snowflake/bigquery/redshift locally you'll need to get credentials and add them to this file. But, to start, you can just focus on postgres tests. They have the best coverage, are the fastest, and are the easiest to set up.

To run the unit tests, use `make test-unit` - it will run the unit tests on python 3.6 and a pep8 linter.

To run the postgres+python 3.6 integration tests, you'll have to do one extra step of setting up the database:

```
docker-compose up -d database
PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres bash test/setup_db.sh
```

And then to actually run them, you can do `make test-quick`.

If you want to see what exactly is getting run on these commands, look at the `Makefile`. Note that the commands start with an `@` which you can ignore, just makefile magic. If you want to see what the involved `tox` commands are using, look at the corresponding `tox.ini` section - hopefully it's pretty self-explanatory.

### Running tests in CI

When a contributor to dbt pushes code, GitHub will trigger a series of CI builds on CircleCI and Appveyor (Windows) to test all of dbt's code. The CI builds trigger all the integration tests, not just postgres+python3.6.

The Snowflake tests take a very long time to run (about an hour), so don't just sit around waiting, it'll be a while!

If you open a PR as a non-contributor, these tests won't run automatically. Someone from the dbt team will reach out to you and get them running after reviewing your code.

## Running dbt locally

Sometimes, you're going to have to pretend to be an end user to reproduce bugs and stuff. So that means manually setting up some stuff that the test harness takes care of for you.

### Installation

First make sure that you setup your `virtualenv` as described in section _Setting up your environment_.

Install dbt (and it's dependencies) with `pip install -r requirements.txt`

What's cool about this mode is any changes you make to the current dbt directory will be reflected immediately in your next `dbt` run.

### Profile

Now you'll also need a 'dbt profile' so dbt can tell how to connect to your database. By default, this file belongs at `~/.dbt/profiles.yml`, so `mkdir ~/.dbt` and then open your favorite text editor and write out something like this to `~/.dbt/profiles.yml`:

```
config:
    send_anonymous_usage_stats: False
    use_colors: True

talk:
    outputs:
        default:
            type: postgres
            threads: 4
            host: localhost
            port: 5432
            user: root
            pass: password
            dbname: postgres
            schema: dbt_postgres
    target: default
```

There's a sample you can look at in the `dbt` [docs](https://docs.getdbt.com/reference#profile) but it's got a lot of extra and as a developer, you really probably only want to test against your local postgres container. The basic idea is that there are multiple 'profiles' (`talk`, in this case) and within those each profile has one or more 'targets' (`default`, in this case), and each profile has a default target. You can specify what profile you want to use with the `--profile` flag, and which target with the `--target` flag. If you want to be really snazzy, dbt project files actually specify their target, and if you match up your dbt project `profile` key with your `profiles.yml` profile names you don't have to use `--profile` (and if you like your profile's default target, no need for `--target` either).

## Example

There is a very simple project that is a very nice example of dbt's capabilities, you can get it from github:

```
cd ~/src/fishtown
git clone git@github.com:fishtown-analytics/talk.git
git checkout use-postgres
```

The `use-postgres` branch configures the project to use your local postgres (instead of the default, Snowflake). You should poke around in this project a bit, particularly the `models` directory.

Before doing anything, let's check the database out:

```
> PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres psql
psql (10.4)
Type "help" for help.

postgres=# \dn
  List of schemas
  Name  |  Owner
--------+----------
 public | postgres
(1 row)

postgres=# \q
```

`\dn` lists schemas in postgres. You can see that we just have the default "public" schema, so we haven't done anything yet.


If you compile your model with `dbt compile` you should see something like this:

```
> dbt compile
Found 2 models, 0 tests, 0 archives, 0 analyses, 59 macros, 1 operations, 1 seed files

09:49:57 | Concurrency: 2 threads (target='default')
09:49:57 |
09:49:57 | Done.
```

So what does that mean? Well:

- `2 models` refers to the contents of the `models` directory
- `59 macros` are the builtin global macros defined by dbt itself
- `1 operations` is the catalog generation operation that runs by default
- `1 seed files` refers to the seed data in `data/moby_dick.csv`

It will create two new folders: One named `dbt_modules`, which is empty for this case, and one named `target`, which has a few things in it:

- A folder named `compiled`, created by dbt looking at your models and your database schema and filling in references (so `models/moby_dick_base.sql` becomes `target/compiled/talk/moby_dick_base.sql` by replacing the `from {{ ref('moby_dick') }}` with `from "dbt_postgres".moby_dick`)
- A file named `graph.gpickle`, which is your project's dependency/reference graph as understood by the `networkx` library.
- A file named `catalog.json`, which is the data dbt has collected about your project (macros used, models/seeds used, and parent/child reference maps)


Next, load the seed file into the database with `dbt seed`, it'll look like this:

```
> dbt seed
Found 2 models, 0 tests, 0 archives, 0 analyses, 59 macros, 1 operations, 1 seed files

10:40:46 | Concurrency: 2 threads (target='default')
10:40:46 |
10:40:46 | 1 of 1 START seed file dbt_postgres.moby_dick........................ [RUN]
10:40:47 | 1 of 1 OK loaded seed file dbt_postgres.moby_dick.................... [INSERT 17774 in 0.44s]
10:40:47 |
10:40:47 | Finished running 1 seeds in 0.65s.

Completed successfully
```

If you go into postgres now, you can see that you have a new schema ('dbt_postgres'), a new table in that schema ('moby_dick'), and a bunch of stuff in that table:

```
> PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres psql
psql (10.4)
Type "help" for help.

postgres=# \dn
     List of schemas
     Name     |  Owner
--------------+----------
 dbt_postgres | root
 public       | postgres
(2 rows)

postgres=# \dt dbt_postgres.*
            List of relations
    Schema    |   Name    | Type  | Owner
--------------+-----------+-------+-------
 dbt_postgres | moby_dick | table | root
(1 row)

postgres=# select count(*) from dbt_postgres.moby_dick;
 count
-------
 17774
(1 row)

postgres=# \q
```

If you run `dbt run` now, you'll see something like this:

```
> dbt run
Found 2 models, 0 tests, 0 archives, 0 analyses, 59 macros, 1 operations, 1 seed files

10:19:41 | Concurrency: 2 threads (target='default')
10:19:41 |
10:19:41 | 1 of 2 START view model dbt_postgres.moby_dick_base.................. [RUN]
10:19:41 | 1 of 2 OK created view model dbt_postgres.moby_dick_base............. [CREATE VIEW in 0.05s]
10:19:41 | 2 of 2 START table model dbt_postgres.word_count..................... [RUN]
10:19:42 | 2 of 2 OK created table model dbt_postgres.word_count................ [SELECT 27390 in 0.19s]
10:19:42 |
10:19:42 | Finished running 1 view models, 1 table models in 0.53s.

Completed successfully

Done. PASS=2 ERROR=0 SKIP=0 TOTAL=2
```

So, some of the same information and then you can see that dbt created a view (`moby_dick_base`) and a table (`word_count`). If you go into postgres, you'll be able to see that!

If you want to inspect the result, you can do so via psql:

```
> PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres psql
psql (10.4)
Type "help" for help.

postgres=# \dt dbt_postgres.*
             List of relations
    Schema    |    Name    | Type  | Owner
--------------+------------+-------+-------
 dbt_postgres | moby_dick  | table | root
 dbt_postgres | word_count | table | root
(2 rows)

postgres=# select * from dbt_postgres.word_count order by ct desc limit 10;
 word |  ct
------+-------
 the  | 13394
      | 12077
 of   |  6368
 and  |  5846
 to   |  4382
 a    |  4377
 in   |  3767
 that |  2753
 his  |  2406
 I    |  1826
(10 rows)
```

It's pretty much what you'd expect - the most common words are "the", "of", "and", etc. (The empty string probably should not be there, but this is just a toy example!)

So what happened here? First, `dbt seed` loaded the data in the csv file into postgres. Then `dbt compile` built out a sort of plan for how everything is linked together by looking up references and macros and the current state of the database. And finally, `dbt run` ran the compiled SQL to generate the word_count table.
