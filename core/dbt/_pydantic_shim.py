# type: ignore

"""Shim to allow support for both Pydantic 1 and Pydantic 2.

dbt-core must support both major versions of Pydantic because dbt-core users might be using an environment with
either version, and we can't restrict them to one or the other. Here, we essentially import all Pydantic objects
from version 1 that we use. Throughout the repo, we import these objects from this file instead of from Pydantic
directly, meaning that we essentially only use Pydantic 1 in dbt-core currently, but without forcing that restriction
on dbt users. The development environment for this repo should be pinned to Pydantic 1 to ensure devs get appropriate
type hints.
"""

from importlib.metadata import version

pydantic_version = version("pydantic")
# Pydantic uses semantic versioning, i.e. <major>.<minor>.<patch>, and we need to know the major
pydantic_major = pydantic_version.split(".")[0]

if pydantic_major == "1":
    from pydantic import BaseSettings  # noqa: F401
elif pydantic_major == "2":
    from pydantic.v1 import BaseSettings  # noqa: F401
else:
    raise RuntimeError(
        f"Currently only pydantic 1 and 2 are supported, found pydantic {pydantic_version}"
    )
