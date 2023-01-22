from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats
from typing import Any, Generator

import yappi


@contextmanager
def profiler(enable: bool, outfile: str) -> Generator[Any, None, None]:
    try:
        if enable:
            profiler = Profile()
            profiler.enable()
            yappi.start()

        yield
    finally:
        if enable:
            profiler.disable()
            stats = Stats(profiler)
            stats.sort_stats("tottime")
            stats.dump_stats(str(outfile))
            yappi.stop()
            threads = yappi.get_thread_stats()
            all_stats = Stats()

            for thread in threads:
                stats = yappi.get_func_stats(ctx_id=thread.id)
                filename = f"thread-{thread.id}.pstat"
                stats.save(filename, "pstat")

                pstats = yappi.convert2pstats(stats)
                all_stats.add(pstats)

            all_stats.dump_stats('threads-combined.pstat')
