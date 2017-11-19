
import json
from dbt.compat import to_string


class ModelHookType:
    PreHook = 'pre-hook'
    PostHook = 'post-hook'
    Both = [PreHook, PostHook]


def _parse_hook_to_dict(hook_string):
    try:
        hook_dict = json.loads(hook_string)
    except ValueError as e:
        hook_dict = {"sql": hook_string}

    if 'transaction' not in hook_dict:
        hook_dict['transaction'] = True

    return hook_dict


def get_hook_dict(hook, index):
    if isinstance(hook, dict):
        hook_dict = hook
    else:
        hook_dict = _parse_hook_to_dict(to_string(hook))

    hook_dict['index'] = index
    return hook_dict


def get_hooks(model, hook_key):
    hooks = model.get('config', {}).get(hook_key, [])

    if not isinstance(hooks, (list, tuple)):
        hooks = [hooks]

    wrapped = [get_hook_dict(hook, i) for i, hook in enumerate(hooks)]
    return wrapped
