from __future__ import annotations

import base64
import hashlib
import secrets
import webbrowser


def generate_pkce() -> tuple[str, str]:
    verifier_bytes = secrets.token_bytes(32)
    verifier = base64.urlsafe_b64encode(verifier_bytes).rstrip(b"=").decode("ascii")
    challenge_hash = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(challenge_hash).rstrip(b"=").decode("ascii")
    return verifier, challenge


def default_opener(url: str) -> None:
    webbrowser.open(url)
