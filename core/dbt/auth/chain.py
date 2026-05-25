from __future__ import annotations

from typing import Optional

from dbt.auth.credentials import Credential
from dbt.auth.errors import AuthError, NotAuthenticated
from dbt.auth.resolvers import (
    CloudYamlResolver,
    EnvVarResolver,
    OAuthInteractiveResolver,
    OAuthPassiveResolver,
)

OAUTH_CLIENT_ID = "854ad54c885f03bbe6ca7eb1e75593fb"


class AuthChain:
    """Ordered chain of credential resolvers, tried in sequence.

    resolve() walks the chain until a resolver returns credentials.
    NotAuthenticated is silently skipped. Any other error is recorded and the
    chain continues; if no credentials are found, the first non-NotAuthenticated
    error is raised — otherwise NotAuthenticated.
    """

    def __init__(self, resolvers: list) -> None:
        self._resolvers = resolvers

    @classmethod
    def default(cls) -> AuthChain:
        """Non-interactive chain: EnvVar -> OAuthPassive -> CloudYaml."""
        return cls(
            resolvers=[
                EnvVarResolver(),
                OAuthPassiveResolver(client_id=OAUTH_CLIENT_ID),
                CloudYamlResolver(),
            ]
        )

    @classmethod
    def interactive(cls) -> AuthChain:
        """Interactive chain: EnvVar -> OAuthPassive -> CloudYaml -> OAuthInteractive."""
        return cls(
            resolvers=[
                EnvVarResolver(),
                OAuthPassiveResolver(client_id=OAUTH_CLIENT_ID),
                CloudYamlResolver(),
                OAuthInteractiveResolver(client_id=OAUTH_CLIENT_ID),
            ]
        )

    def resolve(self) -> Credential:
        """Resolve credentials from the chain.

        Returns the first successful Credential, or raises the most
        actionable AuthError.
        """
        first_error: Optional[AuthError] = None

        for resolver in self._resolvers:
            try:
                return resolver.resolve()
            except NotAuthenticated:
                continue
            except AuthError as e:
                if first_error is None:
                    first_error = e
                continue

        raise first_error if first_error is not None else NotAuthenticated()
