import pytest

from dbt.tracking import (
    disable_tracking,
    initialize_from_flags,
    track_behavior_change_warn,
)
from dbt_common.behavior_flags import Behavior
from dbt_common.events.event_manager_client import (
    add_callback_to_manager,
    cleanup_event_logger,
)


@pytest.fixture
def snowplow_tracker(mocker):
    # initialize `active_user` without writing the cookie to disk
    initialize_from_flags(True, "")
    mocker.patch("dbt.tracking.User.set_cookie").return_value = {"id": 42}

    # add the relevant callback to the event manager
    add_callback_to_manager(track_behavior_change_warn)

    # don't make a call, catch the request
    snowplow_tracker = mocker.patch("dbt.tracking.tracker.track_struct_event")

    yield snowplow_tracker

    # teardown
    cleanup_event_logger()
    disable_tracking()


def test_false_evaluation_triggers_snowplow_tracking(snowplow_tracker):
    behavior = Behavior(
        [{"name": "my_flag", "default": False, "description": "This is a false flag."}], {}
    )
    if behavior.my_flag:
        # trigger a False evaluation
        assert False, "This flag should evaluate to false and skip this line"
    assert snowplow_tracker.called


def test_true_evaluation_does_not_trigger_snowplow_tracking(snowplow_tracker):
    behavior = Behavior(
        [{"name": "my_flag", "default": True, "description": "This is a true flag."}], {}
    )
    if behavior.my_flag:
        pass
    else:
        # trigger a True evaluation
        assert False, "This flag should evaluate to false and skip this line"
    assert not snowplow_tracker.called


def test_false_evaluation_does_not_trigger_snowplow_tracking_when_disabled(snowplow_tracker):
    disable_tracking()

    behavior = Behavior(
        [{"name": "my_flag", "default": False, "description": "This is a false flag."}], {}
    )
    if behavior.my_flag:
        # trigger a False evaluation
        assert False, "This flag should evaluate to false and skip this line"
    assert not snowplow_tracker.called
