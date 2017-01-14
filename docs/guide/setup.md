# Setup

## Installation

### macOS

The preferred way to install dbt on macOS is via [Homebrew](http://brew.sh/). Install homebrew, then run:

```bash
brew update
brew install dbt

# to upgrade dbt, use
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

## Configuration

To create your first dbt project, run:

```bash
› dbt init [project]
```

This will do two things:
- create a directory at `./[project]` with everything you need to get started.
- create a directory at `~/.dbt/` for environment configuration. [TODO]

Finally, configure your environment:
- supply project configuration within `[project]/dbt_project.yml`
- supply environment configuration within `~/.dbt/profiles.yml`

**Please note: `dbt_project.yml` should be checked in to your models repository, so be sure that it does not contain any database
credentials!** All private credentials belong in `~/.dbt/profiles.yml`.
