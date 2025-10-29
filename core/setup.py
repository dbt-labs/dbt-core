#!/usr/bin/env python

"""Legacy setuptools shim retained for compatibility with existing workflows.  Will be removed in a future version."""

from setuptools import setup

# the user has a downlevel version of setuptools.
# ----
# dbt-core uses these packages deeply, throughout the codebase, and there have been breaking changes in past patch releases (even though these are major-version-one).
# Pin to the patch or minor version, and bump in each new minor version of dbt-core.
# ----
# dbt-core uses these packages in standard ways. Pin to the major version, and check compatibility
# with major versions in each new minor version of dbt-core.
# ----
# These packages are major-version-0. Keep upper bounds on upcoming minor versions (which could have breaking changes)
# and check compatibility / bump in each new minor version of dbt-core.
# ----
# These are major-version-0 packages also maintained by dbt-labs.
# Accept patches but avoid automatically updating past a set minor version range.
# Minor versions for these are expected to be backwards-compatible
# ----
# Expect compatibility with all new versions of these packages, so lower bounds only.
# ----

if __name__ == "__main__":
    setup()
