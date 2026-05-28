from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterator


@contextmanager
def secure_open(
    path: Path,
    flags: int = os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
    mode: int = 0o600,
) -> Iterator[IO[str]]:
    """Open a file for writing with restrictive permissions (0o600 by default).

    Used by session_cache and JWKS persistence to avoid leaking tokens via
    world-readable files.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, flags, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yield f
