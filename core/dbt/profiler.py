from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats
from typing import Any, Generator


@contextmanager
def profiler(enable: bool, outfile: str) -> Generator[Any, None, None]:
    try:
        if enable:
            profiler = Profile()
            profiler.enable()

        yield
    finally:
        if enable:
            profiler.disable()
            stats = Stats(profiler)
            stats.sort_stats("tottime")
            stats.dump_stats(str(outfile))
