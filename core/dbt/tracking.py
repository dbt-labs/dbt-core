import os
import platform
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import pytz
import requests
from packaging.version import Version

from dbt import version as dbt_version
from dbt.adapters.exceptions import FailedToConnectError
from dbt.clients.yaml_helper import safe_load, yaml  # noqa:F401
from dbt.events.types import (
    DisableTracking,
    FlushEvents,
    FlushEventsFailure,
    MainEncounteredError,
    SendEventFailure,
    SendingEvent,
    TrackingInitializeFailure,
)
from dbt_common.events.base_types import EventMsg
from dbt_common.events.functions import fire_event, get_invocation_id, msg_to_dict
from dbt_common.exceptions import NotImplementedError


class NOOP:
    def __call__(self, *args, **kwargs):
        return None

    def __getattr__(self, *args, **kwargs):
        return None


class Tracker(NOOP):
    pass


class TimeoutEmitter(NOOP):
    pass


emitter = TimeoutEmitter()
tracker = Tracker()


class User(NOOP):
    pass


active_user: Optional[User] = None


def track(user, *args, **kwargs):
    return


def track_project_id(options):
    return


def track_adapter_info(options):
    return


def track_invocation_start(invocation_context):
    return


def track_project_load(options):
    return


def track_resource_counts(resource_counts):
    return


def track_model_run(options):
    return


def track_rpc_request(options):
    return


def get_base_invocation_context():
    return


def track_package_install(command_name: str, project_hashed_name: Optional[str], options):
    return


def track_deprecation_warn(options):
    return


def track_behavior_change_warn(msg: EventMsg) -> None:
    return


def track_invocation_end(invocation_context, result_type=None):
    return


def track_invalid_invocation(args=None, result_type=None):
    return


def track_experimental_parser_sample(options):
    return


def track_partial_parser(options):
    return


def track_plugin_get_nodes(options):
    return


def track_runnable_timing(options):
    return


def flush():
    fire_event(FlushEvents())
    try:
        tracker.flush()
    except Exception:
        fire_event(FlushEventsFailure())


def disable_tracking():
    return


def do_not_track():
    return


def initialize_from_flags(send_anonymous_usage_stats, profiles_dir):
    return


@contextmanager
def track_run(run_command=None):
    yield
