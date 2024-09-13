import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional
from unittest import mock

from dbt.context.providers import BaseResolver


@contextmanager
def up_one(return_path: Optional[Path] = None):
    current_path = Path.cwd()
    os.chdir("../")
    try:
        yield
    finally:
        os.chdir(return_path or current_path)


def is_aware(dt: datetime) -> bool:
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def patch_microbatch_end_time(dt_str: str):
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return mock.patch.object(BaseResolver, "_build_end_time", return_value=dt)
