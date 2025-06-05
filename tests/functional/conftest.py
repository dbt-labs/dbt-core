import pytest

from tests.functional.fixtures.happy_path_fixture import (  # noqa:D
    happy_path_project,
    happy_path_project_files,
)


@pytest.fixture(scope="function", autouse=True)
def clear_memoized_get_package_with_retries():
    # This fixture is used to clear the memoized cache for _get_package_with_retries
    # in dbt.clients.registry. This is necessary because the cache is shared across
    # tests and can cause unexpected behavior if not cleared as some tests depend on
    # the deprecation warning that _get_package_with_retries fires
    yield
    from dbt.clients.registry import _get_cached

    _get_cached.cache = {}
