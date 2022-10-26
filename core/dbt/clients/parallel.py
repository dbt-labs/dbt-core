from dbt import flags
from threading import Lock as PyodideLock
from threading import RLock as PyodideRLock

if flags.IS_PYODIDE:
    pass  # multiprocessing doesn't work in pyodide
else:
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


class Parallel:
    def __init__(self, is_pyodide: bool) -> None:
        self.Lock = PyodideLock if is_pyodide else MultiprocessingLock
        self.ThreadPool = PyodideThreadPool if is_pyodide else MultiprocessingThreadPool
        self.RLock = PyodideRLock if is_pyodide else MultiprocessingRLock
