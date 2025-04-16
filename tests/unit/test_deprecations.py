import pytest

import dbt.deprecations as deprecations


@pytest.fixture(scope="function")
def active_deprecations():
    deprecations.reset_deprecations()
    assert len(deprecations.active_deprecations) == 0

    yield deprecations.active_deprecations

    deprecations.reset_deprecations()


@pytest.fixture(scope="function")
def buffered_deprecations():
    deprecations.buffered_deprecations.clear()
    assert not deprecations.buffered_deprecations

    yield deprecations.buffered_deprecations

    deprecations.buffered_deprecations.clear()


def test_buffer_deprecation(active_deprecations, buffered_deprecations):
    deprecations.buffer("project-flags-moved")

    assert len(active_deprecations) == 0
    assert len(buffered_deprecations) == 1


def test_fire_buffered_deprecations(active_deprecations, buffered_deprecations):
    deprecations.buffer("project-flags-moved")
    deprecations.fire_buffered_deprecations()

    assert "project-flags-moved" in active_deprecations
    assert len(buffered_deprecations) == 0


def test_can_reset_active_deprecations():
    deprecations.warn("project-flags-moved")
    assert "project-flags-moved" in deprecations.active_deprecations

    deprecations.reset_deprecations()
    assert "project-flags-moved" not in deprecations.active_deprecations


def test_number_of_occurances_is_tracked():
    assert "project-flags-moved" not in deprecations.active_deprecations

    deprecations.warn("project-flags-moved")
    assert "project-flags-moved" in deprecations.active_deprecations
    assert deprecations.active_deprecations["project-flags-moved"] == 1

    deprecations.warn("project-flags-moved")
    assert "project-flags-moved" in deprecations.active_deprecations
    assert deprecations.active_deprecations["project-flags-moved"] == 2
