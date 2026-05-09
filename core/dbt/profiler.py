from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats
from typing import Any, Generator


@contextmanager
def profiler(outfile: str) -> Generator[Any, None, None]:
    profiler = Profile()
    profiler.enable()
    try:
        yield
    finally:
        profiler.disable()
        stats = Stats(profiler)
        stats.sort_stats("tottime")
        stats.dump_stats(str(outfile))
