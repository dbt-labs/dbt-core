from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class CredentialKind(Enum):
    SERVICE_TOKEN = "service_token"
    PAT = "pat"
    OAUTH = "oauth"


@dataclass
class OAuthSession:
    access_token: str
    scopes: list[str]
    expires_at: float  # unix timestamp
    account_host: str
    account_id: int
    user_id: int
    client_id: str
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None


@dataclass
class Credential:
    kind: CredentialKind
    _token: str
    _account_host: str
    _account_id: int
    oauth_session: Optional[OAuthSession] = field(default=None, repr=False)

    @staticmethod
    def from_token(token: str, account_host: str, account_id: int) -> Credential:
        """Classify by prefix: dbtu_ -> Pat, anything else -> ServiceToken."""
        if token.startswith("dbtu_"):
            kind = CredentialKind.PAT
        else:
            kind = CredentialKind.SERVICE_TOKEN
        return Credential(
            kind=kind,
            _token=token,
            _account_host=account_host,
            _account_id=account_id,
        )

    @staticmethod
    def from_oauth(session: OAuthSession) -> Credential:
        return Credential(
            kind=CredentialKind.OAUTH,
            _token=session.access_token,
            _account_host=session.account_host,
            _account_id=session.account_id,
            oauth_session=session,
        )

    @property
    def token(self) -> str:
        return self._token

    @property
    def account_host(self) -> str:
        return self._account_host

    @property
    def account_id(self) -> int:
        return self._account_id
