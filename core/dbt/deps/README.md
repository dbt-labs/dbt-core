# Deps README

The deps module is responsible for installing dbt packages into dbt projects.  A dbt package is a standalone dbt project with models and macros that solve a specific problem area.  More specific information on dbt packages is available on the [docs site](https://docs.getdbt.com/docs/building-a-dbt-project/package-management).


# What's a package?

See [How do I specify a package?](https://docs.getdbt.com/docs/building-a-dbt-project/package-management#how-do-i-specify-a-package) on the docs site for a detailed explination of the different types of packages supported and expected formats.


# Files

## `base.py`

Defines the base classes of `PinnedPackage` and `UnpinnedPackage`.

`downloads_directory` sets the directory packages will be downloaded to.

## `git.py`

Extends `PinnedPackage` and `UnpinnedPackage` specific to dbt packages defined with git urls.

## `local.py`

Extends `PinnedPackage` and `UnpinnedPackage` specific to dbt packages defined locally.

## `registry.py`

Extends `PinnedPackage` and `UnpinnedPackage` specific to dbt packages defined on the dbt Hub registry.

`install` has retry logic if the download or untarring process hit exceptions (see `dbt.utils._connection_exception_retry`).

## `resolver.py`

Resolves the package definition into package objects to download.
