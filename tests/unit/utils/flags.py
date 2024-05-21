import sys
from argparse import Namespace

if sys.version_info < (3, 9):
    from typing import Generator
else:
    from collections.abc import Generator

import pytest

from dbt.flags import set_from_args


@pytest.fixture
def args_for_flags() -> Namespace:
    """Defines the namespace args to be used in `set_from_args` of `set_test_flags` fixture.

    This fixture is meant to be overrided by tests that need specific flags to be set.
    """
    return Namespace()


@pytest.fixture(autouse=True)
def set_test_flags(args_for_flags: Namespace) -> Generator[None, None, None]:
    """Sets up and tears down the global flags for every pytest unit test

    Override `args_for_flags` fixture as needed to set any specific flags.
    """
    set_from_args(args_for_flags, {})
    # fixtures stop setup upon yield
    yield None
    # everything after yield is run at test teardown
    set_from_args(Namespace(), {})
