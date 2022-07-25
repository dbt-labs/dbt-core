from dbt import flags
from threading import Lock as PyodideLock
from threading import RLock as PyodideRLock

if flags.IS_PYODIDE:
    pass  # multiprocessing doesn't work in pyodide
else:
    import multiprocessing
    from multiprocessing.dummy import Pool as MultiprocessingThreadPool
    from multiprocessing.synchronize import Lock as MultiprocessingLock
    from multiprocessing.synchronize import RLock as MultiprocessingRLock


class PyodideThreadPool:
    def __init__(self, num_threads: int) -> None:
        pass

    def close(self):
        pass

    def join(self):
        pass

    def terminate(self):
        pass


if flags.IS_PYODIDE:
    Lock = PyodideLock
    ThreadPool = PyodideThreadPool
    RLock = PyodideRLock
else:
    Lock = MultiprocessingLock
    ThreadPool = MultiprocessingThreadPool
    RLock = MultiprocessingRLock
