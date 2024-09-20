from multiprocessing import get_context
from multiprocessing.context import SpawnContext

_MP_CONTEXT = get_context("spawn")


def get_mp_context() -> SpawnContext:
    return _MP_CONTEXT
