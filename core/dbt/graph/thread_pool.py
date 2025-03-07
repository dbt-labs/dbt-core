from __future__ import annotations

from multiprocessing.pool import ThreadPool


class DbtThreadPool(ThreadPool):
    """A ThreadPool that tracks whether or not it's been closed"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False

    def close(self):
        self.closed = True
        super().close()

    def is_closed(self):
        return self.closed
