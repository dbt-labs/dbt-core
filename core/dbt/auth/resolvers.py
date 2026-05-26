from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import sys
import time
import webbrowser
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml

from dbt.auth.credentials import Credential, OAuthSession
from dbt.auth.errors import (
    AuthenticationExpired,
    InaccessibleSource,
    InteractiveAuthError,
    Malformed,
    NotAuthenticated,
    RefreshFailed,
)
from dbt.auth.session_cache import (
    DBT_HOME_DIR,
    DEFAULT_CACHE_PATH,
    read_session_cache,
    upsert_session,
    write_state_auth,
)
from dbt.clients.yaml_helper import safe_load
from dbt.config.user_settings import set_user_setting_flag

AUTH_SERVER_URL = "https://us1.dbt.com/register"
INTERACTIVE_TIMEOUT = 600  # 10 minutes
OAUTH_SCOPES = "user_access offline_access"

STATE_OAUTH_AUTH_URL = "https://auth.runcache.com"
STATE_OAUTH_TOKEN_URL = "https://auth.runcache.com/token"
STATE_OAUTH_CLIENT_ID = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6"
STATE_OAUTH_SCOPE = "runcache:scope:orgs"


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
            raise Malformed(f"DBT_CLOUD_ACCOUNT_ID {account_id_str!r} is not a valid integer")

        return Credential.from_token(token, host, account_id)


class OAuthPassiveResolver:
    """Resolves credentials from a cached OAuth session — no user interaction.

    Checks ~/.dbt/oauth_sessions.json for a non-expired session matching
    client_id. If the access token is expired but a refresh token is present,
    attempts a token refresh.
    """

    kind = ResolverKind.OAUTH_PASSIVE

    def __init__(
        self,
        client_id: str,
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
            return Credential.from_oauth(non_expired[0])

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
        user_id, account_id, account_host = _decode_access_token(new_access_token)

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

        return Credential.from_oauth(new_session)


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
            raise Malformed(f"failed to parse {cloud_path}: {e}")

        if not isinstance(config, dict):
            raise Malformed(f"expected mapping in {cloud_path}")

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
            raise Malformed(
                "token-value is empty in dbt_cloud.yml; re-download from the dbt platform UI"
            )

        account_id_str = project.get("account-id", "")
        try:
            account_id = int(account_id_str)
        except (ValueError, TypeError):
            raise Malformed(
                f"account-id {account_id_str!r} in {cloud_path} is not a valid integer"
            )

        return Credential.from_token(token_value, project.get("account-host", ""), account_id)


class OAuthInteractiveResolver:
    """Resolves credentials via browser-based PKCE OAuth flow.

    Spins up a local loopback server on a random port, opens a browser to
    the authorization endpoint, waits for the redirect with the auth code,
    exchanges it for tokens, fetches JWKS, and persists the session.
    """

    kind = ResolverKind.OAUTH_INTERACTIVE

    def __init__(
        self,
        client_id: str,
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
        self.opener = opener or _default_opener

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

    def resolve(self) -> Optional[Credential]:
        pkce_verifier, pkce_challenge = _generate_pkce()
        oauth_state = secrets.token_urlsafe(16)

        server = _OAuthCallbackServer(expected_state=oauth_state)
        port = server.server_address[1]
        redirect_url = f"http://localhost:{port}/"

        state_ctx = _build_state_oauth_context(redirect_url)

        params = {
            "redirect_url": redirect_url,
            "client_id": self.client_id,
            "code_challenge": pkce_challenge,
            "state": oauth_state,
            "scope": self.scopes,
            "response_type": "code",
            "code_challenge_method": "S256",
            "dbt_state_oauth": state_ctx["encoded_param"],
        }
        auth_url = f"{self.auth_server_url.rstrip('/')}?{urlencode(params)}"

        server.timeout = self.timeout

        if os.environ.get("DBT_SKIP_BROWSER_AUTH", "").strip():
            print(f"Open this URL to authenticate:\n{auth_url}", file=sys.stderr)
        else:
            print("Opening browser for dbt platform login...", file=sys.stderr)
            self.opener(auth_url)

        server.handle_request()
        server.server_close()

        if server.error:
            raise InteractiveAuthError(server.error)
        if server.result is None:
            raise InteractiveAuthError(
                f"interactive authentication timed out after {self.timeout}s"
            )

        if self._is_callback_from_state(server.result):
            return self._resolve_dbt_state_auth_from_callback(server.result, state_ctx)

        return self._resolve_dbt_platform_auth_from_callback(
            server.result, pkce_verifier, redirect_url
        )

    @staticmethod
    def _is_callback_from_state(result: dict) -> bool:
        return bool(result.get("dbt_state_oauth"))

    def _resolve_dbt_state_auth_from_callback(self, result: dict, state_ctx: dict):
        state_token_data = _exchange_state_code(
            token_url=state_ctx["token_url"],
            client_id=state_ctx["client_id"],
            code=result["dbt_state_oauth"],
            redirect_uri=state_ctx["redirect_uri"],
            code_verifier=state_ctx["code_verifier"],
        )
        write_state_auth(state_token_data)
        set_user_setting_flag("manage_state", True)

    def _resolve_dbt_platform_auth_from_callback(
        self, result: dict, pkce_verifier: str, redirect_url: str
    ) -> Credential:
        code = result["code"]
        account_url = result["account_url"]

        token_data = _exchange_code(
            account_url=account_url,
            client_id=self.client_id,
            code=code,
            redirect_url=redirect_url,
            code_verifier=pkce_verifier,
        )

        user_id, account_id, account_host = _decode_access_token(token_data["access_token"])

        _fetch_and_persist_jwks(account_host)

        scopes = token_data.get("scope", "").split()
        expires_in = token_data.get("expires_in", 3600)

        session = OAuthSession(
            access_token=token_data["access_token"],
            refresh_token=token_data.get("refresh_token"),
            id_token=token_data.get("id_token"),
            scopes=scopes,
            expires_at=time.time() + expires_in,
            account_host=account_host,
            account_id=account_id,
            user_id=user_id,
            client_id=self.client_id,
        )

        upsert_session(session, self.cache_path)

        return Credential.from_oauth(session)


def _build_state_oauth_context(redirect_url: str) -> dict:
    client_id = (
        os.environ.get("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", "").strip()
        or os.environ.get("RUN_CACHE_OAUTH_CLIENT_ID", "").strip()
        or STATE_OAUTH_CLIENT_ID
    )
    auth_url = os.environ.get("RUN_CACHE_AUTH_URL", "").strip() or STATE_OAUTH_AUTH_URL
    token_url = os.environ.get("RUN_CACHE_TOKEN_URL", "").strip() or STATE_OAUTH_TOKEN_URL
    verifier, challenge = _generate_pkce()
    state = secrets.token_urlsafe(16)

    authorize_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_url,
        "scope": STATE_OAUTH_SCOPE,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    authorize_url = f"{auth_url.rstrip('/')}?{urlencode(authorize_params)}"
    return {
        "encoded_param": base64.urlsafe_b64encode(authorize_url.encode()).decode(),
        "code_verifier": verifier,
        "client_id": client_id,
        "token_url": token_url,
        "redirect_uri": redirect_url,
    }


def _exchange_state_code(
    token_url: str,
    client_id: str,
    code: str,
    redirect_uri: str,
    code_verifier: str,
) -> dict:
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    try:
        resp = requests.post(token_url, data=form, timeout=30)
        if not resp.ok:
            raise InteractiveAuthError(
                f"dbt State token exchange failed: HTTP {resp.status_code} — {resp.text}"
            )
    except requests.ConnectionError as e:
        raise InteractiveAuthError(f"dbt State token exchange failed: {e}")

    try:
        return resp.json()
    except ValueError as e:
        raise InteractiveAuthError(f"invalid dbt State token response: {e}")


def _default_opener(url: str) -> None:
    webbrowser.open(url)


def _generate_pkce() -> tuple[str, str]:
    """Generate a PKCE code verifier and challenge pair."""
    verifier_bytes = secrets.token_bytes(32)
    verifier = base64.urlsafe_b64encode(verifier_bytes).rstrip(b"=").decode("ascii")
    challenge_hash = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(challenge_hash).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _decode_access_token(token: str) -> tuple[int, int, str]:
    """Extract (user_id, account_id, account_host) from a JWT access token.

    Only decodes the payload — does NOT verify the signature (that's the
    server's job; we just need the claims).
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise InteractiveAuthError("access token is not a valid JWT")

    payload_b64 = parts[1]
    # JWT uses base64url without padding; add padding back
    padding = 4 - len(payload_b64) % 4
    if padding != 4:
        payload_b64 += "=" * padding

    try:
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)
    except (ValueError, json.JSONDecodeError) as e:
        raise InteractiveAuthError(f"failed to decode JWT payload: {e}")

    try:
        user_id = int(payload["sub"])
    except (KeyError, ValueError) as e:
        raise InteractiveAuthError(f"invalid 'sub' claim: {e}")

    try:
        account_id = int(payload["https://dbt.com/account_id"])
    except (KeyError, ValueError) as e:
        raise InteractiveAuthError(f"invalid account_id claim: {e}")

    iss = payload.get("iss", "")
    parsed = urlparse(iss)
    account_host = parsed.hostname
    if not account_host:
        raise InteractiveAuthError(f"cannot extract host from JWT 'iss': {iss}")

    return user_id, account_id, account_host


def _exchange_code(
    account_url: str,
    client_id: str,
    code: str,
    redirect_url: str,
    code_verifier: str,
) -> dict:
    token_url = f"{account_url.rstrip('/')}/oauth/token"
    redirect_uri_with_account = f"{redirect_url}?{urlencode({'account_url': account_url})}"
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri_with_account,
    }
    try:
        resp = requests.post(token_url, data=form, timeout=30)
        if not resp.ok:
            raise InteractiveAuthError(
                f"token exchange failed: HTTP {resp.status_code} — {resp.text}"
            )
    except requests.ConnectionError as e:
        raise InteractiveAuthError(f"token exchange failed: {e}")

    try:
        return resp.json()
    except ValueError as e:
        raise InteractiveAuthError(f"invalid token response: {e}")


def _fetch_and_persist_jwks(account_host: str) -> None:
    """Fetch JWKS from the account host and persist to ~/.dbt/jwks.{host}.json."""
    jwks_url = f"https://{account_host}/.well-known/jwks.json"
    try:
        resp = requests.get(jwks_url, timeout=10)
        resp.raise_for_status()
        jwks_data = resp.json()
    except requests.RequestException:
        return

    jwks_data["fetched_at"] = time.time()
    target = DBT_HOME_DIR / f"jwks.{account_host}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(jwks_data, indent=2), encoding="utf-8")


class _OAuthCallbackServer(HTTPServer):
    def __init__(self, expected_state: str):
        super().__init__(("localhost", 0), _OAuthCallbackHandler)
        self.expected_state = expected_state
        self.result: Optional[dict] = None
        self.error: Optional[str] = None


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    server: _OAuthCallbackServer

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if "error" in params:
            error = params["error"][0]
            desc = params.get("error_description", [""])[0]
            message = f"{error}: {desc}" if desc else error
            self.server.error = message
            self._send_response(
                500,
                f"<h1>Error</h1><p>{message}</p>",
            )
            return

        if params.get("state", [None])[0] != self.server.expected_state:
            self.server.error = "invalid OAuth state parameter"
            self._send_response(500, "<h1>Error</h1><p>Invalid state</p>")
            return

        dbt_state_code = params.get("dbt_state_oauth", [None])[0]
        code = params.get("code", [None])[0]
        account_url = params.get("account_url", [None])[0]

        if dbt_state_code:
            self.server.result = {"dbt_state_oauth": dbt_state_code}
        elif code and account_url:
            self.server.result = {"code": code, "account_url": account_url}
        elif not code:
            self.server.error = "redirect missing code parameter"
            self._send_response(500, "<h1>Error</h1><p>Missing code</p>")
            return
        else:
            self.server.error = "redirect missing account_url parameter"
            self._send_response(500, "<h1>Error</h1><p>Missing account_url</p>")
            return

        self._send_response(
            200,
            "<h1>Success</h1><p>You have logged in. You can close this window.</p>",
        )

    def _send_response(self, status: int, body: str):
        html = f"<!doctype html><html><head><meta charset='UTF-8'/><title>dbt - Login</title></head><body style='text-align:center;font-family:sans-serif'>{body}</body></html>"
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        pass
