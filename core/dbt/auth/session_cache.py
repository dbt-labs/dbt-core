from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from dbt.auth.credentials import OAuthSession
from dbt.exceptions import InaccessibleSource, Malformed


def _dbt_home_dir() -> Path:
    from dbt.cli.resolvers import default_dbt_home_dir

    return default_dbt_home_dir()


DBT_HOME_DIR = _dbt_home_dir()
DEFAULT_CACHE_PATH = DBT_HOME_DIR / "oauth_sessions.json"
STATE_AUTH_PATH = DBT_HOME_DIR / "state_auth.json"


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

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache.to_dict(), indent=2), encoding="utf-8")


def write_state_auth(token_data: dict, path: Path = STATE_AUTH_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(token_data, indent=2), encoding="utf-8")


def read_state_auth(path: Path = STATE_AUTH_PATH) -> dict | None:
    try:
        data = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as e:
        raise InaccessibleSource(str(path), e)

    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as e:
        raise Malformed(f"invalid JSON in {path}: {e}")

    if not isinstance(parsed, dict):
        raise Malformed(f"expected object in {path}, got {type(parsed).__name__}")

    return parsed


def remove_state_auth(path: Path = STATE_AUTH_PATH) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
