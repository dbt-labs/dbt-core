import json
from typing import Any, Dict, Union

from dbt_common.dataclass_schema import StrEnum


class ModelHookType(StrEnum):
    PreHook = "pre-hook"
    PostHook = "post-hook"


def get_hook_dict(source: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """From a source string-or-dict, get a dictionary that can be passed to
    Hook.from_dict
    """
    if isinstance(source, dict):
        return source
    try:
        return json.loads(source)
    except ValueError:
        return {"sql": source}
