import pytest

import dbt.deprecations as deprecations
from dbt.events.types import ProjectFlagsMovedDeprecation
from dbt_common.events.event_manager_client import add_callback_to_manager
from dbt_common.events.types import Note
from tests.utils import EventCatcher


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


class PreviewedDeprecation(deprecations.DBTDeprecation):
    _name = "previewed-deprecation"
    _event = "ProjectFlagsMovedDeprecation"
    _is_preview = True


class TestPreviewDeprecation:

    @pytest.fixture(scope="class", autouse=True)
    def deprecations_list_and_deprecations(self):
        deprecations.deprecations_list.append(PreviewedDeprecation())
        deprecations.deprecations["previewed-deprecation"] = PreviewedDeprecation()

        yield

        for dep in deprecations.deprecations_list:
            if dep._name == "previewed-deprecation":
                deprecations.deprecations_list.remove(dep)
                break
        deprecations.deprecations.pop("previewed-deprecation")

    def test_preview_deprecation(self):
        pfmd_catcher = EventCatcher(event_to_catch=ProjectFlagsMovedDeprecation)
        add_callback_to_manager(pfmd_catcher.catch)
        note_catcher = EventCatcher(event_to_catch=Note)
        add_callback_to_manager(note_catcher.catch)

        deprecations.warn(
            "previewed-deprecation",
        )
        assert "previewed-deprecation" not in deprecations.active_deprecations
        assert len(pfmd_catcher.caught_events) == 0
        assert len(note_catcher.caught_events) == 1
