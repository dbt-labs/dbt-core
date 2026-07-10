from dbt.flags import get_flags
from dbt.tracking import track_hint_view
from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.functions import fire_event
from dbt_common.events.types import Note

# Hint message text shown to the user. Keep these actionable and point at docs.
REUSE_RELATIONS_ON_TOO_MANY_MODELS = (
    "[HINT] You're building a lot from scratch. Did you know you can speed up your "
    "builds by reusing relations from other schemas: check out "
    "https://docs.getdbt.com/docs/optimizing-builds?utm_source=dbt-cli"
)
LONG_PARSING_WITHOUT_V2_PARSER = (
    "[HINT] Your parse is taking a long time. Did you know you can speed up your "
    "parsing with the new rust parser: check out "
    "https://docs.getdbt.com/reference/global-configs/parsing?utm_source=dbt-cli#opt-in-v2-parser"
)


class HintType(StrEnum):
    REUSE_RELATIONS_ON_TOO_MANY_MODELS = "reuse_relations_on_too_many_models"
    LONG_PARSING_WITHOUT_V2_PARSER = "long_parsing_without_v2_parser"


hint_to_msg_map: dict[HintType, str] = {
    HintType.REUSE_RELATIONS_ON_TOO_MANY_MODELS: REUSE_RELATIONS_ON_TOO_MANY_MODELS,
    HintType.LONG_PARSING_WITHOUT_V2_PARSER: LONG_PARSING_WITHOUT_V2_PARSER,
}


def show_hint(hint_type: HintType) -> None:
    """Surface a hint to the user, unless hints have been disabled via the
    hints_enabled flag. Also records a telemetry event so we can tell which
    hints are actually being seen."""
    if not getattr(get_flags(), "HINTS_ENABLED", True):
        return

    msg = hint_to_msg_map[hint_type]
    fire_event(Note(msg=msg))
    track_hint_view(hint_type)
