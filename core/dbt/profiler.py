from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats


@contextmanager
def profiler(enable, outfile):
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
            stats.dump_stats(outfile)
