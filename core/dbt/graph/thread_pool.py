from __future__ import annotations

from math import floor
from multiprocessing.pool import ThreadPool


class DbtThreadPool(ThreadPool):
    """A ThreadPool that tracks whether or not it's been closed"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False
        # The maximum number of threads that can be used by the pool
        # Used for determining how MicrobatchBatchRungners should be run
        self.max_threads = kwargs.get("processes") if kwargs.get("processes") else args[0]
        # The maximum number of microbatch models that can be run concurrently
        # Used for determining if a MicrobatchModelRunner can be submitted to the pool
        self.max_microbatch_models = max(1, floor(self.max_threads / 2))

    def close(self):
        self.closed = True
        super().close()

    def is_closed(self):
        return self.closed
