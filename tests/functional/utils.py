import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def up_one():
    current_path = Path.cwd()
    os.chdir("../")
    try:
        yield
    finally:
        os.chdir(current_path)
