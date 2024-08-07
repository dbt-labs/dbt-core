import pytest

import dbt.deprecations as deprecations


@pytest.fixture(scope="function")
def active_deprecations():
    assert not deprecations.active_deprecations

    yield deprecations.active_deprecations

    deprecations.reset_deprecations()


@pytest.fixture(scope="function")
def buffered_deprecations():
    assert not deprecations.buffered_deprecations

    yield deprecations.buffered_deprecations

    deprecations.buffered_deprecations.clear()


def test_buffer_deprecation(active_deprecations, buffered_deprecations):
    deprecations.buffer("project-flags-moved")

    assert active_deprecations == set()
    assert len(buffered_deprecations) == 1


def test_fire_buffered_deprecations(active_deprecations, buffered_deprecations):
    deprecations.buffer("project-flags-moved")
    deprecations.fire_buffered_deprecations()

    assert active_deprecations == set(["project-flags-moved"])
    assert len(buffered_deprecations) == 0
