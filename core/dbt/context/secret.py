import os
from typing import Any, Dict, Optional

from .base import BaseContext, contextmember

from dbt.exceptions import raise_parsing_error


class SecretContext(BaseContext):
    """This context is used in profiles.yml + packages.yml. It can render secret
    env vars that aren't usable elsewhere"""

    @contextmember
    def env_var(self, var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.

        In this context *only*, env_var will return the actual values of
        env vars prefixed with DBT_ENV_SECRET_
        """
        return_value = None
        if var in os.environ:
            return_value = os.environ[var]
        elif default is not None:
            return_value = default

        if return_value is not None:
            self.env_vars[var] = return_value
            return return_value
        else:
            msg = f"Env var required but not provided: '{var}'"
            raise_parsing_error(msg)


def generate_secret_context(cli_vars: Dict[str, Any]) -> Dict[str, Any]:
    ctx = SecretContext(cli_vars)
    # This is not a Mashumaro to_dict call
    return ctx.to_dict()
