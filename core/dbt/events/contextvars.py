import contextlib
import contextvars

from typing import Any, Generator, Mapping, Dict


LOG_PREFIX = "log_"
TASK_PREFIX = "task_"

_context_vars: Dict[str, contextvars.ContextVar] = {}


def get_contextvars(prefix: str) -> Dict[str, Any]:
    rv = {}
    ctx = contextvars.copy_context()

    prefix_len = len(prefix)
    for k in ctx:
        if k.name.startswith(prefix) and ctx[k] is not Ellipsis:
            rv[k.name[prefix_len:]] = ctx[k]

    return rv


def get_node_info():
    cvars = get_contextvars(LOG_PREFIX)
    if "node_info" in cvars:
        return cvars["node_info"]
    else:
        return {}


def get_project_root():
    cvars = get_contextvars(TASK_PREFIX)
    if "project_root" in cvars:
        return cvars["project_root"]
    else:
        return None


def clear_contextvars(prefix: str) -> None:
    ctx = contextvars.copy_context()
    for k in ctx:
        if k.name.startswith(prefix):
            k.set(Ellipsis)


def set_log_contextvars(**kwargs: Any) -> Mapping[str, contextvars.Token]:
    return set_contextvars(LOG_PREFIX, **kwargs)


def set_task_contextvars(**kwargs: Any) -> Mapping[str, contextvars.Token]:
    return set_contextvars(TASK_PREFIX, **kwargs)


# put keys and values into context. Returns the contextvar.Token mapping
# Save and pass to reset_contextvars
def set_contextvars(prefix: str, **kwargs: Any) -> Mapping[str, contextvars.Token]:
    cvar_tokens = {}
    for k, v in kwargs.items():
        log_key = f"{prefix}{k}"
        try:
            var = _context_vars[log_key]
        except KeyError:
            var = contextvars.ContextVar(log_key, default=Ellipsis)
            _context_vars[log_key] = var

        cvar_tokens[k] = var.set(v)

    return cvar_tokens


# reset by Tokens
def reset_contextvars(prefix: str, **kwargs: contextvars.Token) -> None:
    for k, v in kwargs.items():
        log_key = f"{prefix}{k}"
        var = _context_vars[log_key]
        var.reset(v)


# remove from contextvars
def unset_contextvars(prefix: str, *keys: str) -> None:
    for k in keys:
        if k in _context_vars:
            log_key = f"{prefix}{k}"
            _context_vars[log_key].set(Ellipsis)


# Context manager or decorator to set and unset the context vars
@contextlib.contextmanager
def log_contextvars(**kwargs: Any) -> Generator[None, None, None]:
    context = get_contextvars(LOG_PREFIX)
    saved = {k: context[k] for k in context.keys() & kwargs.keys()}

    set_contextvars(LOG_PREFIX, **kwargs)
    try:
        yield
    finally:
        unset_contextvars(LOG_PREFIX, *kwargs.keys())
        set_contextvars(LOG_PREFIX, **saved)


# Context manager for earlier in task.run
@contextlib.contextmanager
def task_contextvars(**kwargs: Any) -> Generator[None, None, None]:
    context = get_contextvars(TASK_PREFIX)
    saved = {k: context[k] for k in context.keys() & kwargs.keys()}

    set_contextvars(TASK_PREFIX, **kwargs)
    try:
        yield
    finally:
        unset_contextvars(TASK_PREFIX, *kwargs.keys())
        set_contextvars(TASK_PREFIX, **saved)
