from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats
from typing import Any, Generator


@contextmanager
def profiler(outfile: str) -> Generator[Any, None, None]:
    """Context manager that profiles the enclosed block and writes stats to outfile.

    Use conditionally at the call site instead of passing an ``enable`` flag::

        if should_profile:
            with profiler("output.prof"):
                do_work()
    """
    p = Profile()
    try:
        p.enable()
        yield
    finally:
        p.disable()
        stats = Stats(p)
        stats.sort_stats("tottime")
        stats.dump_stats(str(outfile))
