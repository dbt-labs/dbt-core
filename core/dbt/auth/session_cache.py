from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

from dbt.auth.credentials import OAuthSession
from dbt.auth.errors import InaccessibleSource, Malformed

DBT_HOME_DIR = Path.home() / ".dbt"
DEFAULT_CACHE_PATH = DBT_HOME_DIR / "oauth_sessions.json"


@dataclass
class OAuthSessionCache:
    version: int = 1
    sessions: list[OAuthSession] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "sessions": [asdict(s) for s in self.sessions],
        }

    @staticmethod
    def from_dict(data: dict) -> OAuthSessionCache:
        sessions = [OAuthSession(**s) for s in data.get("sessions", [])]
        return OAuthSessionCache(
            version=data.get("version", 1),
            sessions=sessions,
        )


def read_session_cache(path: Path = DEFAULT_CACHE_PATH) -> OAuthSessionCache:
    try:
        data = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return OAuthSessionCache()
    except OSError as e:
        raise InaccessibleSource(str(path), e)

    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as e:
        raise Malformed(f"invalid JSON in {path}: {e}")

    if not isinstance(parsed, dict):
        raise Malformed(f"expected object in {path}, got {type(parsed).__name__}")

    return OAuthSessionCache.from_dict(parsed)


def _write_atomic(content: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=target.parent,
        prefix=".oauth_sessions_",
        suffix=".tmp",
        delete=False,
    ) as f:
        tmp_path = Path(f.name)
        f.write(content)

    try:
        if os.name != "nt":
            tmp_path.chmod(0o600)
        tmp_path.replace(target)
    except OSError:
        tmp_path.unlink(missing_ok=True)
        raise


def upsert_session(session: OAuthSession, path: Path = DEFAULT_CACHE_PATH) -> None:
    cache = read_session_cache(path)

    existing = next(
        (
            i
            for i, s in enumerate(cache.sessions)
            if s.client_id == session.client_id and s.account_id == session.account_id
        ),
        None,
    )
    if existing is not None:
        cache.sessions[existing] = session
    else:
        cache.sessions.append(session)

    _write_atomic(json.dumps(cache.to_dict(), indent=2), path)


def remove_session(client_id: str, account_id: int, path: Path = DEFAULT_CACHE_PATH) -> None:
    cache = read_session_cache(path)
    cache.sessions = [
        s for s in cache.sessions if not (s.client_id == client_id and s.account_id == account_id)
    ]
    _write_atomic(json.dumps(cache.to_dict(), indent=2), path)
