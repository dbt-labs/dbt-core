import uuid

_INVOCATION_ID = str(uuid.uuid4())


def get_invocation_id() -> str:
    return _INVOCATION_ID


def reset_invocation_id():
    global _INVOCATION_ID
    _INVOCATION_ID = str(uuid.uuid4())
