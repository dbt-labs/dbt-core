import os
from dataclasses import dataclass
from typing import Optional

from dbt.cli import params
from dbt.deprecations import warn
from dbt_common.constants import ENGINE_ENV_PREFIX


@dataclass(frozen=True)
class EngineEnvVar:
    name: str
    old_name: Optional[str] = None


def _create_engine_env_var(name: str) -> EngineEnvVar:
    if name.startswith(ENGINE_ENV_PREFIX):
        return EngineEnvVar(name=name)
    elif name.startswith("DBT"):
        return EngineEnvVar(name=name.replace("DBT", f"{ENGINE_ENV_PREFIX}"), old_name=name)
    else:
        raise RuntimeError(
            f"Invalid environment variable: {name}, this will only happen if we add a new option to dbt that has an envvar that doesn't start with DBT_ or {ENGINE_ENV_PREFIX}"
        )


# This will be needed later in the flags module to cross propagate old non-engine prefixed to engine prefixed env vars (and vice versa)
KNOWN_ENGINE_ENV_VARS: set[EngineEnvVar] = {
    _create_engine_env_var(envvar) for envvar in params.KNOWN_ENV_VARS
}
_ALLOWED_ENV_VARS: set[str] = {envvar.name for envvar in KNOWN_ENGINE_ENV_VARS}


def validate_engine_env_vars() -> None:
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
