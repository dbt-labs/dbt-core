import os

from core.dbt.deprecations import warn
from dbt_common.constants import ENGINE_ENV_PREFIX

_ALLOWED_ENV_VARS: set[str] = set()


def validate_env_var() -> None:
    """
    Validate that any set environment variables that begin with the engine prefix are allowed.
    """
    for env_var in os.environ.keys():
        if env_var.startswith(ENGINE_ENV_PREFIX) and env_var not in _ALLOWED_ENV_VARS:
            warn(
                "environment-variable-namespace-deprecation",
                env_var=env_var,
                reserved_prefix=ENGINE_ENV_PREFIX,
            )
