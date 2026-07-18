import json
import time
from functools import lru_cache
from pathlib import Path

from dbt.constants import DBT_HOME_DIR_NAME
from dbt.flags import get_flags
from dbt.tracking import track_hint_view
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

# Prefix prepended to every hint when surfaced to the user.
HINT_PREFIX = "[HINT] "

# Don't show the same hint more than once a week.
HINT_COOLDOWN_SECONDS = 7 * 24 * 60 * 60

# File (under the dbt home dir, ~/.dbt) tracking when each hint was last shown.
# Living in the home dir means the cooldown is shared across every project, so a
# hint doesn't reappear just because you ran dbt in a different project.
HINT_TS_FILENAME = "hint_ts.json"

# Hint message text shown to the user. Keep these actionable and point at docs.
REUSE_RELATIONS_ON_TOO_MANY_MODELS = (
    "You're rebuilding a lot from scratch. You can use state to skip models that haven't changed: "
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


def _hint_ts_file() -> Path:
    # A single, fixed location (~/.dbt/hint_ts.json). Because it's shared across
    # every project, the once-a-week cooldown follows the user, not the project.
    return Path.home() / DBT_HOME_DIR_NAME / HINT_TS_FILENAME


@lru_cache(maxsize=None)
def _read_hint_ts(path_str: str) -> dict:
    """Read the hint file (hint_type -> last-shown epoch seconds), cached by path
    so repeated cooldown checks don't re-touch disk. The returned dict is mutated
    in place by record_hint_shown(), keeping the cache current within a run;
    keying by path also isolates different homes (e.g. across tests). A missing or
    partially-written file just means "never shown"."""
    try:
        return json.loads(Path(path_str).read_text())
    except Exception:
        # Missing, unreadable (permissions), or partially-written file — none of
        # it should break a run, so treat any read failure as "never shown".
        return {}


def has_hint_cooldown(hint_type: HintType) -> bool:
    """True if this hint was shown recently enough that we should stay quiet."""
    last_shown = _read_hint_ts(str(_hint_ts_file())).get(hint_type, 0)
    return (time.time() - last_shown) < HINT_COOLDOWN_SECONDS


def record_hint_shown(hint_type: HintType) -> None:
    """Stamp the current time for this hint and persist it, so future runs honor
    the cooldown."""
    path = _hint_ts_file()
    hint_ts = _read_hint_ts(str(path))
    # Store epoch seconds as an int so the file interops cleanly with the Rust
    # engine (which reads/writes integer timestamps). Mutating the cached dict in
    # place keeps a subsequent cooldown check consistent without a re-read.
    hint_ts[hint_type] = int(time.time())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(hint_ts))
    except Exception:
        # A read-only/full/permission-denied home dir must never break the run
        # just because we couldn't persist a hint timestamp.
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
