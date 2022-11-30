import contextlib
import contextvars

from typing import Any, Generator, Mapping, Dict
from dbt.events.proto_types import NodeInfo


LOG_PREFIX = "log_"
LOG_PREFIX_LEN = len(LOG_PREFIX)

_log_context_vars: Dict[str, contextvars.ContextVar] = {}


def get_contextvars() -> Dict[str, Any]:
    rv = {}
    ctx = contextvars.copy_context()

    for k in ctx:
        if k.name.startswith(LOG_PREFIX) and ctx[k] is not Ellipsis:
            rv[k.name[LOG_PREFIX_LEN:]] = ctx[k]

    return rv


def get_node_info():
    cvars = get_contextvars()
    if "node_info" in cvars:
        return cvars["node_info"]
    else:
        return NodeInfo()


def clear_contextvars() -> None:
    ctx = contextvars.copy_context()
    for k in ctx:
        if k.name.startswith(LOG_PREFIX):
            k.set(Ellipsis)


# put keys and values into context. Returns the contextvar.Token mapping
# Save and pass to reset_contextvars
def set_contextvars(**kwargs: Any) -> Mapping[str, contextvars.Token]:
    cvar_tokens = {}
    for k, v in kwargs.items():
        log_key = f"{LOG_PREFIX}{k}"
        try:
            var = _log_context_vars[log_key]
        except KeyError:
            var = contextvars.ContextVar(log_key, default=Ellipsis)
            _log_context_vars[log_key] = var

        cvar_tokens[k] = var.set(v)

    return cvar_tokens


# reset by Tokens
def reset_contextvars(**kwargs: contextvars.Token) -> None:
    for k, v in kwargs.items():
        log_key = f"{LOG_PREFIX}{k}"
        var = _log_context_vars[log_key]
        var.reset(v)


# remove from contextvars
def unset_contextvars(*keys: str) -> None:
    for k in keys:
        if k in _log_context_vars:
            log_key = f"{LOG_PREFIX}{k}"
            _log_context_vars[log_key].set(Ellipsis)


# Context manager or decorator to set and unset the context vars
@contextlib.contextmanager
def log_contextvars(**kwargs: Any) -> Generator[None, None, None]:
    context = get_contextvars()
    saved = {k: context[k] for k in context.keys() & kwargs.keys()}

    set_contextvars(**kwargs)
    try:
        yield
    finally:
        unset_contextvars(*kwargs.keys())
        set_contextvars(**saved)
