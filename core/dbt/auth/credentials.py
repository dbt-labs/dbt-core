from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


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
    token: str
    expires_at: float

    @property
    def valid(self) -> bool:
        return bool(self.token) and not self.expired

    @property
    def expired(self) -> bool:
        return time.time() >= self.expires_at

    def apply(self, headers: dict) -> None:
        headers["Authorization"] = f"Bearer {self.token}"


@dataclass
class PlatformCredential(Credential):
    account_host: str = ""
    account_id: int = 0
    oauth_session: Optional[OAuthSession] = field(default=None, repr=False)

    @staticmethod
    def from_token(token: str, account_host: str, account_id: int) -> PlatformCredential:
        return PlatformCredential(
            token=token,
            expires_at=float("inf"),
            account_host=account_host,
            account_id=account_id,
        )

    @staticmethod
    def from_oauth(session: OAuthSession) -> PlatformCredential:
        return PlatformCredential(
            token=session.access_token,
            expires_at=session.expires_at,
            account_host=session.account_host,
            account_id=session.account_id,
            oauth_session=session,
        )


@dataclass
class StateCredential(Credential):
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None
    scopes: list[str] = field(default_factory=list)
    client_id: str = ""
    user_id: str = ""
    email: str = ""
    name: str = ""

    @staticmethod
    def from_token_response(token_data: dict) -> StateCredential:
        expires_in = token_data.get("expires_in", 900)
        return StateCredential(
            token=token_data["access_token"],
            expires_at=time.time() + expires_in,
            refresh_token=token_data.get("refresh_token"),
            id_token=token_data.get("id_token"),
            scopes=token_data.get("scope", "").split(),
            client_id=token_data.get("client_id", ""),
        )
