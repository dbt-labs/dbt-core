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


@pytest.fixture(autouse=True)
def clear_buffered_deprecations():
    # buffered_deprecations is a module-level list that can retain stale entries
    # across tests in the same process. This happens when Flags.__init__ buffers
    # a deprecation (via normalize_warn_error_options) but then raises before
    # fire_buffered_deprecations() is called to drain and clear the buffer.
    # The stale entry then fires in the next test's invocation, potentially
    # causing spurious failures (e.g. WEOIncludeExcludeDeprecation + --warn-error).
    from dbt.deprecations import buffered_deprecations

    buffered_deprecations.clear()
