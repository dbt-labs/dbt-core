from hologram.helpers import StrEnum
import json

from dbt.contracts.graph.parsed import Hook

from typing import Union, Dict, Any


class ModelHookType(StrEnum):
    PreHook = 'pre-hook'
    PostHook = 'post-hook'


def get_hook_dict(source: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """From a source string-or-dict, get a dictionary that can be passed to
    Hook.from_dict
    """
    if isinstance(source, dict):
        return source
    try:
        return json.loads(source)
    except ValueError:
        return {'sql': source}


def get_hook(source, index):
    hook_dict = get_hook_dict(source)
    hook_dict.setdefault('index', index)
    return Hook.from_dict(hook_dict)
