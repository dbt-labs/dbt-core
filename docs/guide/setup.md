# Setup

## Installation

### macOS

The preferred way to install dbt on macOS is via [Homebrew](http://brew.sh/). Install Homebrew, then run:

```bash
brew update
brew install dbt
```

To upgrade dbt, use

```bash
brew update
brew upgrade dbt
```

If you'd prefer to use the development version of dbt, you can install it as follows. Please note that the development version is considered unstable, and may contain bugs.

```bash
brew update
brew install --HEAD dbt

brew update
brew reinstall --HEAD dbt
```

To install from source (not recommended), first install `postgresql` with Homebrew:

```bash
brew install postgresql
```

Then, install using pip:

```bash
pip install dbt
```

If you encounter SSL cryptography errors during install, make sure your copy of pip is up-to-date (via [cryptography.io](https://cryptography.io/en/latest/faq/#compiling-cryptography-on-os-x-produces-a-fatal-error-openssl-aes-h-file-not-found-error))

```bash
pip install -U pip
pip install -U dbt
```

### Ubuntu / Debian

First, make sure you have postgres installed:

```bash
sudo apt-get install libpq-dev python-dev
```

Then, install using `pip`:

```bash
pip install dbt
```

### Windows

Before starting, you'll need to install [Git for Windows](https://git-scm.com/downloads) and [Python version 3.5 or higher for Windows](https://www.python.org/downloads/windows/).

Open up Windows powershell as an administrator, and install dbt with `pip`:

```bash
pip install dbt
```

## Create a new dbt project

To create your first dbt project, run:

```bash
› dbt init [project]
```

This will create a directory at `./[project]` with almost everything you need to get started.

Finally, configure your environment:

- supply project configuration within `[project]/dbt_project.yml`
- supply environment configuration within `~/.dbt/profiles.yml`

**Please note: `dbt_project.yml` should be checked in to your models repository, so be sure that it does not contain any database
credentials!** All private credentials belong in `~/.dbt/profiles.yml`.


## Configuring a project

The `dbt_project.yml` file is responsible for configuring how dbt operates on your project.
You can find a sample `dbt_project.yml` file [here](https://github.com/fishtown-analytics/dbt/blob/master/sample.dbt_project.yml).


## Configuring profiles

`profiles.yml` defines how dbt connects to your data warehouse(s). You can find a sample
`profiles.yml` file [here](https://github.com/fishtown-analytics/dbt/blob/master/sample.profiles.yml).
By default, dbt expects this file to be located at `~/.dbt/profiles.yml`, but this location can be changed
on the command line with the `--profiles-dir` option.
