from __future__ import annotations

import base64
import os
import secrets
from urllib.parse import urlencode, urlparse, urlunparse

import requests

from dbt.auth.credentials import StateCredential
from dbt.auth.oauth.platform import generate_pkce
from dbt.auth.session_cache import write_state_auth
from dbt.config.user_settings import set_user_setting_flag
from dbt.exceptions import InteractiveAuthError
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

STATE_OAUTH_AUTH_URL = "https://auth.state.dbt.com"
STATE_OAUTH_TOKEN_URL = "https://auth.state.dbt.com/token"
STATE_OAUTH_CLIENT_ID = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6"
STATE_OAUTH_SCOPE = "runcache:scope:orgs"


def build_context(redirect_url: str) -> dict:
    client_id = (
        os.environ.get("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", "").strip()
        or os.environ.get("RUN_CACHE_OAUTH_CLIENT_ID", "").strip()
        or STATE_OAUTH_CLIENT_ID
    )
    auth_url = os.environ.get("RUN_CACHE_AUTH_URL", "").strip() or STATE_OAUTH_AUTH_URL
    token_url = os.environ.get("RUN_CACHE_TOKEN_URL", "").strip() or STATE_OAUTH_TOKEN_URL
    verifier, challenge = generate_pkce()
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
    parsed = urlparse(auth_url.rstrip("/"))
    authorize_url = urlunparse(parsed._replace(query=urlencode(authorize_params)))
    return {
        "encoded_param": base64.b64encode(authorize_url.encode()).decode(),
        "code_verifier": verifier,
        "client_id": client_id,
        "token_url": token_url,
        "redirect_uri": redirect_url,
        "state": state,
    }


def exchange_code(
    token_url: str,
    client_id: str,
    code: str,
    redirect_uri: str,
    code_verifier: str,
    state: str,
) -> dict:
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
        "state": state,
    }
    try:
        resp = requests.post(token_url, data=form, timeout=30)
        if not resp.ok:
            raise InteractiveAuthError(
                f"dbt State token exchange failed: HTTP {resp.status_code} — {resp.text}"
            )
    except requests.RequestException as e:
        raise InteractiveAuthError(f"dbt State token exchange failed: {e}")

    try:
        return resp.json()
    except ValueError as e:
        raise InteractiveAuthError(f"invalid dbt State token response: {e}")


def resolve_from_callback(result: dict, ctx: dict) -> StateCredential:
    token_data = exchange_code(
        token_url=ctx["token_url"],
        client_id=ctx["client_id"],
        code=result["dbt_state_oauth"],
        redirect_uri=ctx["redirect_uri"],
        code_verifier=ctx["code_verifier"],
        state=ctx["state"],
    )
    write_state_auth(token_data)
    return StateCredential.from_token_response(token_data)


def on_state_login_success(credential: StateCredential) -> None:
    """Post-login steps after a successful dbt State OAuth login.

    - Unconditionally sets manage_state: true in user_settings.yml (no prompt).
    """
    set_user_setting_flag("manage_state", True)
    fire_event(
        Note(
            msg="dbt State login successful.\n"
            "dbt State enabled. Configuration written to ~/.dbt/user_settings.yml."
        )
    )
