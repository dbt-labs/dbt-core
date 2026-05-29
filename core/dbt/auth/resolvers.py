from __future__ import annotations

import os
import time
import webbrowser
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
import yaml

from dbt.auth.credentials import (
    Credential,
    OAuthSession,
    PlatformCredential,
    StateCredential,
)
from dbt.auth.oauth.callback_server import OAuthCallbackServer
from dbt.auth.oauth.platform import build_context as build_platform_oauth_context
from dbt.auth.oauth.platform import decode_access_token
from dbt.auth.oauth.platform import resolve_from_callback as resolve_platform_auth
from dbt.auth.oauth.state import build_context as build_state_oauth_context
from dbt.auth.oauth.state import resolve_from_callback as resolve_state_auth
from dbt.auth.session_cache import (
    DBT_HOME_DIR,
    DEFAULT_CACHE_PATH,
    read_session_cache,
    upsert_session,
)
from dbt.clients.yaml_helper import safe_load
from dbt.exceptions import (
    AuthenticationExpired,
    InaccessibleSource,
    InteractiveAuthError,
    MalformedAuthConfig,
    NotAuthenticated,
    RefreshFailed,
)
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

AUTH_SERVER_URL = "https://us1.dbt.com/register"
INTERACTIVE_TIMEOUT = 600  # 10 minutes
OAUTH_CLIENT_ID = "854ad54c885f03bbe6ca7eb1e75593fb"
OAUTH_SCOPES = "account:read identity:read offline_access"


class ResolverKind(Enum):
    ENV_VAR = "env_var"
    OAUTH_PASSIVE = "oauth_passive"
    CLOUD_YAML = "cloud_yaml"
    OAUTH_INTERACTIVE = "oauth_interactive"


class EnvVarResolver:
    """Resolves credentials from DBT_CLOUD_ACCOUNT_HOST, DBT_CLOUD_TOKEN, DBT_CLOUD_ACCOUNT_ID."""

    kind = ResolverKind.ENV_VAR

    def resolve(self) -> Credential:
        host = os.environ.get("DBT_CLOUD_ACCOUNT_HOST", "").strip()
        token = os.environ.get("DBT_CLOUD_TOKEN", "").strip()
        account_id_str = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "").strip()

        if not host or not token or not account_id_str:
            raise NotAuthenticated()

        try:
            account_id = int(account_id_str)
        except ValueError:
            raise MalformedAuthConfig(
                f"DBT_CLOUD_ACCOUNT_ID {account_id_str!r} is not a valid integer"
            )

        return PlatformCredential.from_token(token, host, account_id)


class OAuthPassiveResolver:
    """Resolves credentials from a cached OAuth session — no user interaction.

    Checks ~/.dbt/oauth_sessions.json for a non-expired session matching
    client_id. If the access token is expired but a refresh token is present,
    attempts a token refresh.
    """

    kind = ResolverKind.OAUTH_PASSIVE

    def __init__(
        self,
        client_id: str = OAUTH_CLIENT_ID,
        cache_path: Optional[Path] = None,
        token_endpoint_override: Optional[str] = None,
    ) -> None:
        self.client_id = client_id
        self.cache_path = cache_path or DEFAULT_CACHE_PATH
        self.token_endpoint_override = token_endpoint_override

    def resolve(self) -> Credential:
        cache = read_session_cache(self.cache_path)
        matching = [s for s in cache.sessions if s.client_id == self.client_id]

        if not matching:
            raise NotAuthenticated()

        now = time.time()
        non_expired = [s for s in matching if s.expires_at > now]

        if non_expired:
            return PlatformCredential.from_oauth(non_expired[0])

        refreshable = next((s for s in matching if s.refresh_token is not None), None)
        if refreshable is None:
            raise AuthenticationExpired()

        return self._refresh(refreshable)

    def _refresh_token_url(self, account_host: str) -> str:
        if self.token_endpoint_override:
            base = self.token_endpoint_override.rstrip("/")
        else:
            base = f"https://{account_host}"
        return f"{base}/oauth/token"

    def _refresh(self, session: OAuthSession) -> Credential:
        url = self._refresh_token_url(session.account_host)
        form = {
            "grant_type": "refresh_token",
            "refresh_token": session.refresh_token,
            "client_id": self.client_id,
            "scope": " ".join(session.scopes) if session.scopes else OAUTH_SCOPES,
        }
        try:
            resp = requests.post(url, data=form, timeout=30)
        except requests.RequestException as e:
            raise RefreshFailed(str(e))

        if 400 <= resp.status_code < 500:
            raise AuthenticationExpired()

        if not resp.ok:
            raise RefreshFailed(f"token refresh request failed: HTTP {resp.status_code}")

        try:
            token_data = resp.json()
        except ValueError as e:
            raise RefreshFailed(f"invalid token response: {e}")

        new_access_token = token_data["access_token"]
        user_id, account_id, account_host = decode_access_token(new_access_token)

        scopes = token_data.get("scope", "").split()
        expires_in = token_data.get("expires_in", 3600)

        new_session = OAuthSession(
            access_token=new_access_token,
            refresh_token=token_data.get("refresh_token"),
            id_token=token_data.get("id_token"),
            scopes=scopes,
            expires_at=time.time() + expires_in,
            account_host=account_host,
            account_id=account_id,
            user_id=user_id,
            client_id=self.client_id,
        )

        # Atomic write BEFORE returning — refresh tokens are one-time-use.
        upsert_session(new_session, self.cache_path)

        return PlatformCredential.from_oauth(new_session)


class CloudYamlResolver:
    """Resolves credentials from ~/.dbt/dbt_cloud.yml (read-only).

    Resolution for active project/host:
    1. dbt_cloud.yml context block (active-project, active-host)
    2. Environment variable overrides (DBT_CLOUD_PROJECT_ID, DBT_CLOUD_ACCOUNT_HOST)

    Credentials (token, account-id) are read from the matched project entry.
    """

    kind = ResolverKind.CLOUD_YAML

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path

    def _default_path(self) -> Optional[Path]:
        return DBT_HOME_DIR / "dbt_cloud.yml"

    def resolve(self) -> Credential:
        cloud_path = self.path or self._default_path()
        if cloud_path is None:
            raise NotAuthenticated()

        try:
            content = cloud_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise NotAuthenticated()
        except OSError as e:
            raise InaccessibleSource(str(cloud_path), e)

        try:
            config = safe_load(content)
        except (ValueError, yaml.YAMLError) as e:
            raise MalformedAuthConfig(f"failed to parse {cloud_path}: {e}")

        if not isinstance(config, dict):
            raise MalformedAuthConfig(f"expected mapping in {cloud_path}")

        context = config.get("context", {})
        projects = config.get("projects", [])

        active_project = os.environ.get("DBT_CLOUD_PROJECT_ID", "").strip() or context.get(
            "active-project", ""
        )
        active_host = os.environ.get("DBT_CLOUD_ACCOUNT_HOST", "").strip() or context.get(
            "active-host", ""
        )

        project = next(
            (
                p
                for p in projects
                if str(p.get("project-id", "")) == str(active_project)
                and p.get("account-host", "") == active_host
            ),
            None,
        )

        if project is None:
            raise NotAuthenticated()

        token_value = project.get("token-value", "")
        if not token_value:
            raise MalformedAuthConfig(
                "token-value is empty in dbt_cloud.yml; re-download from the dbt platform UI"
            )

        account_id_str = project.get("account-id", "")
        try:
            account_id = int(account_id_str)
        except (ValueError, TypeError):
            raise MalformedAuthConfig(
                f"account-id {account_id_str!r} in {cloud_path} is not a valid integer"
            )

        return PlatformCredential.from_token(
            token_value, project.get("account-host", ""), account_id
        )


class OAuthInteractiveResolver:
    """Resolves credentials via browser-based PKCE OAuth flow.

    Spins up a local loopback server on a random port, opens a browser to
    the authorization endpoint, waits for the redirect with the auth code,
    exchanges it for tokens, fetches JWKS, and persists the session.
    """

    kind = ResolverKind.OAUTH_INTERACTIVE

    def __init__(
        self,
        client_id: str = OAUTH_CLIENT_ID,
        cache_path: Optional[Path] = None,
        auth_server_url: Optional[str] = None,
        scopes: str = OAUTH_SCOPES,
        timeout: int = INTERACTIVE_TIMEOUT,
        opener: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.client_id = client_id
        self.cache_path = cache_path or DEFAULT_CACHE_PATH
        self.auth_server_url = (
            auth_server_url
            or os.environ.get("DBT_CLOUD_STAGING_URL", "").strip()
            or self._auth_server_from_cloud_yaml()
            or AUTH_SERVER_URL
        )
        self.scopes = scopes
        self.timeout = timeout
        self.opener = opener or webbrowser.open

    @staticmethod
    def _auth_server_from_cloud_yaml() -> Optional[str]:
        """If dbt_cloud.yml exists and has an active-host, use that cell's auth server."""
        cloud_path = DBT_HOME_DIR / "dbt_cloud.yml"
        try:
            config = safe_load(cloud_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError):
            return None

        if not isinstance(config, dict):
            return None

        host = (config.get("context") or {}).get("active-host", "").strip()
        if host:
            return f"https://{host}"
        return None

    def resolve(self) -> PlatformCredential | StateCredential:
        server = OAuthCallbackServer()
        port = server.server_address[1]
        redirect_url = f"http://localhost:{port}/"

        platform_ctx = build_platform_oauth_context(
            redirect_url=redirect_url,
            client_id=self.client_id,
            auth_server_url=self.auth_server_url,
            scopes=self.scopes,
        )
        server.platform_oauth_state = platform_ctx["state"]

        state_ctx = build_state_oauth_context(redirect_url)
        server.state_oauth_state = state_ctx["state"]

        parsed = urlparse(platform_ctx["authorize_url"])
        params = parse_qsl(parsed.query)
        params.append(("dbt_state_oauth", state_ctx["encoded_param"]))
        params.append(("_dbtsrc", "dbt-core"))
        auth_url = urlunparse(parsed._replace(query=urlencode(params)))

        server.timeout = self.timeout

        # TODO: remove lazy import once dbt.cli.__init__ circular dep is resolved
        # dbt.flags -> dbt.cli.main -> dbt.auth.resolvers
        from dbt.flags import get_flags

        skip_browser = getattr(get_flags(), "SKIP_BROWSER_AUTH", False) or False
        fire_event(Note(msg=f"Opening your browser to complete login...\n{auth_url}"))
        if not skip_browser:
            try:
                self.opener(auth_url)
            except Exception:
                fire_event(
                    Note(
                        msg="Cannot open browser. Please paste the URL above into your browser to authorize the dbt CLI."
                    )
                )
        fire_event(
            Note(
                msg="If you need to reset your password, complete the reset, then re-run dbt login to finish authenticating."
            )
        )

        server.handle_request()
        server.server_close()

        if server.error:
            raise InteractiveAuthError(server.error)
        if server.result is None:
            raise InteractiveAuthError(
                f"interactive authentication timed out after {self.timeout}s"
            )

        if "dbt_state_oauth" in server.result:
            return resolve_state_auth(server.result, state_ctx)

        return resolve_platform_auth(
            server.result,
            client_id=self.client_id,
            pkce_verifier=platform_ctx["code_verifier"],
            redirect_url=redirect_url,
            cache_path=self.cache_path,
        )
