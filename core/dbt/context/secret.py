import os
from typing import Any, Dict, Optional

from .base import BaseContext, contextmember

from dbt.constants import SECRET_ENV_PREFIX, DEFAULT_ENV_PLACEHOLDER
from dbt.exceptions import EnvVarMissingError


SECRET_PLACEHOLDER = "$$$DBT_SECRET_START$$${}$$$DBT_SECRET_END$$$"


class SecretContext(BaseContext):
    """This context is used in profiles.yml + packages.yml. It can render secret
    env vars that aren't usable elsewhere"""

    @contextmember()
    def env_var(self, var: str, default: Optional[str] = None) -> str:
        """The env_var() function. Return the environment variable named 'var'.
        If there is no such environment variable set, return the default.

        If the default is None, raise an exception for an undefined variable.

        In this context *only*, env_var will accept env vars prefixed with DBT_ENV_SECRET.
        It will return the name of the secret env var, wrapped in 'start' and 'end' identifiers.
        The actual value will be subbed in later in SecretRenderer.render_value()
        """
        return_value = None

        # if this is a 'secret' env var, just return the name of the env var
        # instead of rendering the actual value here, to avoid any risk of
        # Jinja manipulation. it will be subbed out later, in SecretRenderer.render_value
        if var in os.environ and var.startswith(SECRET_ENV_PREFIX):
            return SECRET_PLACEHOLDER.format(var)

        elif var in os.environ:
            return_value = os.environ[var]
        elif default is not None:
            return_value = default

        if return_value is not None:
            # store env vars in the internal manifest to power partial parsing
            # if it's a 'secret' env var, we shouldn't even get here
            # but just to be safe — don't save secrets
            if not var.startswith(SECRET_ENV_PREFIX):
                # If the environment variable is set from a default, store a string indicating
                # that so we can skip partial parsing.  Otherwise the file will be scheduled for
                # reparsing. If the default changes, the file will have been updated and therefore
                # will be scheduled for reparsing anyways.
                self.env_vars[var] = return_value if var in os.environ else DEFAULT_ENV_PLACEHOLDER
            return return_value
        else:
            raise EnvVarMissingError(var)


def generate_secret_context(cli_vars: Dict[str, Any]) -> Dict[str, Any]:
    ctx = SecretContext(cli_vars)
    # This is not a Mashumaro to_dict call
    return ctx.to_dict()
