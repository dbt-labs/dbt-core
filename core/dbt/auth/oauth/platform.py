from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse

import click
import requests

from dbt.auth.credentials import OAuthSession, PlatformCredential
from dbt.auth.secure_file import secure_open
from dbt.auth.session_cache import DBT_HOME_DIR, DEFAULT_CACHE_PATH, upsert_session
from dbt.config.user_settings import get_user_setting_flag, set_user_setting_flag
from dbt.exceptions import InteractiveAuthError
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note


class DbtPlatformAPIClient:
    def __init__(self, credential: PlatformCredential) -> None:
        self.credential = credential
        self._base_url = f"https://{credential.account_host}"
        self._headers = {"Authorization": f"Bearer {credential.token}"}

    def is_state_configured(self) -> bool:
        url = f"{self._base_url}/api/private/accounts/{self.credential.account_id}/features/"
        try:
            resp = requests.get(url, headers=self._headers, timeout=5)
            resp.raise_for_status()
            return resp.json().get("data", {}).get("dbt-state", False) is True
        except (requests.RequestException, ValueError, KeyError):
            return False

    def warm_license_cache(self) -> None:
        url = (
            f"{self._base_url}/api/private/accounts/{self.credential.account_id}/feature-licenses/"
        )
        try:
            requests.post(url, headers=self._headers, timeout=5)
        except requests.RequestException:
            pass

    def fetch_and_persist_jwks(self) -> None:
        jwks_url = f"{self._base_url}/.well-known/jwks.json"
        try:
            resp = requests.get(jwks_url, timeout=10)
            resp.raise_for_status()
            jwks_data = resp.json()
        except requests.RequestException:
            return

        jwks_data["fetched_at"] = time.time()
        target = DBT_HOME_DIR / f"jwks.{self.credential.account_host}.json"
        with secure_open(target) as f:
            json.dump(jwks_data, f, indent=2)


def generate_pkce() -> tuple[str, str]:
    verifier_bytes = secrets.token_bytes(32)
    verifier = base64.urlsafe_b64encode(verifier_bytes).rstrip(b"=").decode("ascii")
    challenge_hash = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(challenge_hash).rstrip(b"=").decode("ascii")
    return verifier, challenge


def build_context(
    redirect_url: str,
    client_id: str,
    auth_server_url: str,
    scopes: str,
) -> dict:
    verifier, challenge = generate_pkce()
    state = secrets.token_urlsafe(16)

    params = {
        "redirect_url": redirect_url,
        "client_id": client_id,
        "code_challenge": challenge,
        "state": state,
        "scope": scopes,
        "response_type": "code",
        "code_challenge_method": "S256",
    }
    authorize_url = f"{auth_server_url.rstrip('/')}?{urlencode(params)}"
    return {
        "authorize_url": authorize_url,
        "code_verifier": verifier,
        "state": state,
    }


def decode_access_token(token: str) -> tuple[int, int, str]:
    """Extract (user_id, account_id, account_host) from a JWT access token.

    Only decodes the payload — does NOT verify the signature.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise InteractiveAuthError("access token is not a valid JWT")

    payload_b64 = parts[1]
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


def exchange_code(
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
    except requests.RequestException as e:
        raise InteractiveAuthError(f"token exchange failed: {e}")

    try:
        return resp.json()
    except ValueError as e:
        raise InteractiveAuthError(f"invalid token response: {e}")


def resolve_from_callback(
    result: dict,
    client_id: str,
    pkce_verifier: str,
    redirect_url: str,
    cache_path: Optional[Path] = None,
) -> PlatformCredential:
    token_data = exchange_code(
        account_url=result["account_url"],
        client_id=client_id,
        code=result["code"],
        redirect_url=redirect_url,
        code_verifier=pkce_verifier,
    )

    user_id, account_id, account_host = decode_access_token(token_data["access_token"])

    scopes = token_data.get("scope", "").split()
    expires_in = token_data.get("expires_in", 3600)

    session = OAuthSession(
        access_token=token_data["access_token"],
        refresh_token=token_data.get("refresh_token"),
        id_token=token_data.get("id_token"),
        scopes=scopes,
        expires_at=int(time.time() + expires_in),
        account_host=account_host,
        account_id=account_id,
        user_id=user_id,
        client_id=client_id,
    )

    upsert_session(session, cache_path or DEFAULT_CACHE_PATH)

    credential = PlatformCredential.from_oauth(session)
    DbtPlatformAPIClient(credential).fetch_and_persist_jwks()

    return credential


def on_platform_login_success(credential: PlatformCredential) -> None:
    """Post-login steps after a successful platform OAuth login.

    - manage_state already enabled (resolved flag): no change.
    - manage_state explicitly false in user_settings.yml: prompts before overriding.
    - manage_state not set: auto-enables without prompting.
    """
    # Lazy import: dbt.flags -> dbt.cli.main -> dbt.auth.oauth.platform
    from dbt.flags import get_flags

    client = DbtPlatformAPIClient(credential)
    client.warm_license_cache()

    fire_event(Note(msg="Congratulations! You are now signed in."))

    configured = client.is_state_configured()
    state_enabled_locally = getattr(get_flags(), "MANAGE_STATE", False) or False

    if state_enabled_locally:
        if not configured:
            fire_event(
                Note(
                    msg=(
                        "Looks like dbt State is enabled on this machine but not in your "
                        "dbt platform account. To enable State in your platform account, see docs: "
                        "https://docs.getdbt.com/docs/deploy/dbt-state-setup"
                    )
                )
            )
    elif get_user_setting_flag("manage_state") is False:
        confirmed = click.confirm(
            "dbt State is currently disabled in ~/.dbt/user_settings.yml. "
            "Enable state on this machine by default, for faster and cheaper builds?",
            default=True,
        )
        if confirmed:
            set_user_setting_flag("manage_state", True)
            fire_event(
                Note(msg="dbt State enabled. Configuration written to ~/.dbt/user_settings.yml.")
            )
            if not configured:
                fire_event(
                    Note(
                        msg=(
                            "Looks like dbt State is enabled on this machine but not in your "
                            "dbt platform account. To enable State in your platform account, see docs: "
                            "https://docs.getdbt.com/docs/deploy/dbt-state-setup"
                        )
                    )
                )
        else:
            fire_event(
                Note(
                    msg="If you want to enable dbt State in the future, "
                    "you can modify ~/.dbt/user_settings.yml"
                )
            )
    else:
        set_user_setting_flag("manage_state", True)
        fire_event(
            Note(msg="dbt State enabled. Configuration written to ~/.dbt/user_settings.yml.")
        )
        if not configured:
            fire_event(
                Note(
                    msg=(
                        "Looks like dbt State is enabled on this machine but not in your "
                        "dbt platform account. To enable State in your platform account, see docs: "
                        "https://docs.getdbt.com/docs/deploy/dbt-state-setup"
                    )
                )
            )
