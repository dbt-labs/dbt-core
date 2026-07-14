import json
import time
from contextvars import ContextVar
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union

from dbt.flags import get_flags
from dbt.tracking import track_hint_view
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

# Prefix prepended to every hint when surfaced to the user.
HINT_PREFIX = "[HINT] "

# Don't show the same hint more than once a week.
HINT_COOLDOWN_SECONDS = 7 * 24 * 60 * 60

# File (under the project's target dir) tracking when each hint was last shown.
HINT_TS_FILENAME = "hint_ts.json"

# Hint message text shown to the user. Keep these actionable and point at docs.
REUSE_RELATIONS_ON_TOO_MANY_MODELS = (
    "You're rebuilding a lot from scratch. You can use state to save time and money by reusing or skipping existing objects: "
    "https://docs.getdbt.com/docs/optimize-builds?utm_source=dbt-cli"
)

LONG_PARSING_WITHOUT_V2_PARSER = (
    "Your parse is taking a long time. You can speed up your parsing with the new rust parser: "
    "https://docs.getdbt.com/reference/global-configs/parsing?utm_source=dbt-cli#opt-in-v2-parser"
)


class HintType(StrEnum):
    REUSE_RELATIONS_ON_TOO_MANY_MODELS = "reuse_relations_on_too_many_models"
    LONG_PARSING_WITHOUT_V2_PARSER = "long_parsing_without_v2_parser"


hint_to_msg_map: dict[HintType, str] = {
    HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS: REUSE_RELATIONS_ON_TOO_MANY_MODELS,
    HintType.LONG_PARSING_WITHOUT_V2_PARSER: LONG_PARSING_WITHOUT_V2_PARSER,
}

# Path to the current invocation's hint file, set by load_hint_ts(). Held in a
# ContextVar so it's isolated per invocation and follows dbt's threaded execution
# the same way the invocation context does. None until load_hint_ts() runs.
_hint_ts_path: ContextVar[Optional[Path]] = ContextVar("hint_ts_path", default=None)


@lru_cache(None)
def _read_hint_ts(path_str: str) -> dict:
    """Read and cache one hint file's contents, keyed by path. The returned dict
    is mutated in place by record_hint_shown(), so the cache stays current within
    this process; a new invocation is a new process and re-reads from disk."""
    try:
        return json.loads(Path(path_str).read_text())
    except (FileNotFoundError, ValueError):
        # No file yet, or a partially-written one — treat as "never shown".
        return {}


def load_hint_ts(target_path: Optional[Union[str, Path]] = None) -> dict:
    """Point the hint machinery at a project's target dir and load its hint file.
    Call this early (from requires.runtime_config); later reads reuse the cached
    copy so we don't touch disk on every hint."""
    if target_path is None:
        return {}
    path = Path(target_path) / HINT_TS_FILENAME
    _hint_ts_path.set(path)
    return _read_hint_ts(str(path))


def reset_hint_ts() -> None:
    """Forget the current path and clear cached hint files. Primarily for tests,
    where the process (and this cache) is reused across invocations."""
    _hint_ts_path.set(None)
    _read_hint_ts.cache_clear()


def has_hint_cooldown(hint_type: HintType) -> bool:
    """True if this hint was shown recently enough that we should stay quiet."""
    path = _hint_ts_path.get()
    if path is None:
        return False
    last_shown = _read_hint_ts(str(path)).get(hint_type, 0)
    return (time.time() - last_shown) < HINT_COOLDOWN_SECONDS


def record_hint_shown(hint_type: HintType) -> None:
    """Stamp the current time for this hint and persist it, so future runs honor
    the cooldown. No-op if we never loaded a target path (nowhere to write)."""
    path = _hint_ts_path.get()
    if path is None:
        return
    hint_ts = _read_hint_ts(str(path))
    # Store epoch seconds as an int so the file interops cleanly with the Rust
    # engine (which reads/writes integer timestamps).
    hint_ts[hint_type] = int(time.time())
    try:
        path.write_text(json.dumps(hint_ts))
    except OSError:
        # A read-only/full/unwritable target must never break the run just
        # because we couldn't persist a hint timestamp.
        pass


def show_hint(hint_type: HintType) -> None:
    """Surface a hint to the user, unless hints have been disabled via the
    hints_enabled flag or the hint is still within its cooldown window. Also
    records a telemetry event so we can tell which hints are actually being seen."""
    if not getattr(get_flags(), "HINTS_ENABLED", True):
        return
    if has_hint_cooldown(hint_type):
        return

    msg = hint_to_msg_map[hint_type]
    fire_event(Note(msg=f"{HINT_PREFIX}{msg}"))
    track_hint_view(hint_type)
    record_hint_shown(hint_type)
