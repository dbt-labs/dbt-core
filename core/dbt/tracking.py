from typing import Optional

from dbt.clients.yaml_helper import (  # noqa:F401
    yaml,
    safe_load,
    Loader,
    Dumper,
)
from dbt.events.functions import fire_event, get_invocation_id
from dbt.events.types import (
    DisableTracking,
    SendingEvent,
    SendEventFailure,
    FlushEvents,
    FlushEventsFailure,
    TrackingInitializeFailure,
)
from dbt import version as dbt_version
from dbt import flags
from snowplow_tracker import Subject, Tracker, Emitter, logger as sp_logger
from snowplow_tracker import SelfDescribingJson
from datetime import datetime

import logbook
import pytz
import platform
import uuid
import requests
import os

sp_logger.setLevel(100)

COLLECTOR_URL = "fishtownanalytics.sinter-collect.com"
COLLECTOR_PROTOCOL = "https"

INVOCATION_SPEC = "iglu:com.dbt/invocation/jsonschema/1-0-2"
PLATFORM_SPEC = "iglu:com.dbt/platform/jsonschema/1-0-0"
RUN_MODEL_SPEC = "iglu:com.dbt/run_model/jsonschema/1-0-1"
INVOCATION_ENV_SPEC = "iglu:com.dbt/invocation_env/jsonschema/1-0-0"
PACKAGE_INSTALL_SPEC = "iglu:com.dbt/package_install/jsonschema/1-0-0"
RPC_REQUEST_SPEC = "iglu:com.dbt/rpc_request/jsonschema/1-0-1"
DEPRECATION_WARN_SPEC = "iglu:com.dbt/deprecation_warn/jsonschema/1-0-0"
LOAD_ALL_TIMING_SPEC = "iglu:com.dbt/load_all_timing/jsonschema/1-0-3"
RESOURCE_COUNTS = "iglu:com.dbt/resource_counts/jsonschema/1-0-0"
EXPERIMENTAL_PARSER = "iglu:com.dbt/experimental_parser/jsonschema/1-0-0"
PARTIAL_PARSER = "iglu:com.dbt/partial_parser/jsonschema/1-0-1"
RUNNABLE_TIMING = "iglu:com.dbt/runnable/jsonschema/1-0-0"
DBT_INVOCATION_ENV = "DBT_INVOCATION_ENV"


class TimeoutEmitter(Emitter):
    def __init__(self):
        super().__init__(
            COLLECTOR_URL,
            protocol=COLLECTOR_PROTOCOL,
            buffer_size=30,
            on_failure=self.handle_failure,
            method="post",
            # don't set this.
            byte_limit=None,
        )

    @staticmethod
    def handle_failure(num_ok, unsent):
        # num_ok will always be 0, unsent will always be 1 entry long, because
        # the buffer is length 1, so not much to talk about
        fire_event(DisableTracking())
        disable_tracking()

    def _log_request(self, request, payload):
        sp_logger.info(f"Sending {request} request to {self.endpoint}...")
        sp_logger.debug(f"Payload: {payload}")

    def _log_result(self, request, status_code):
        msg = f"{request} request finished with status code: {status_code}"
        if self.is_good_status_code(status_code):
            sp_logger.info(msg)
        else:
            sp_logger.warning(msg)

    def http_post(self, payload):
        self._log_request("POST", payload)

        r = requests.post(
            self.endpoint,
            data=payload,
            headers={"content-type": "application/json; charset=utf-8"},
            timeout=5.0,
        )

        self._log_result("GET", r.status_code)
        return r

    def http_get(self, payload):
        self._log_request("GET", payload)

        r = requests.get(self.endpoint, params=payload, timeout=5.0)

        self._log_result("GET", r.status_code)
        return r


emitter = TimeoutEmitter()
tracker = Tracker(
    emitter,
    namespace="cf",
    app_id="dbt",
)


class User:
    def __init__(self, cookie_dir):
        self.do_not_track = True
        self.cookie_dir = cookie_dir

        self.id = None
        self.invocation_id = get_invocation_id()
        self.run_started_at = datetime.now(tz=pytz.utc)

    def state(self):
        return "do not track" if self.do_not_track else "tracking"

    @property
    def cookie_path(self):
        return os.path.join(self.cookie_dir, ".user.yml")

    def initialize(self):
        self.do_not_track = False

        cookie = self.get_cookie()
        self.id = cookie.get("id")

        subject = Subject()
        subject.set_user_id(self.id)
        tracker.set_subject(subject)

    def disable_tracking(self):
        self.do_not_track = True
        self.id = None
        self.cookie_dir = None
        tracker.set_subject(None)

    def set_cookie(self):
        # If the user points dbt to a profile directory which exists AND
        # contains a profiles.yml file, then we can set a cookie. If the
        # specified folder does not exist, or if there is not a profiles.yml
        # file in this folder, then an inconsistent cookie can be used. This
        # will change in every dbt invocation until the user points to a
        # profile dir file which contains a valid profiles.yml file.
        #
        # See: https://github.com/dbt-labs/dbt-core/issues/1645

        user = {"id": str(uuid.uuid4())}

        cookie_path = os.path.abspath(self.cookie_dir)
        profiles_file = os.path.join(cookie_path, "profiles.yml")
        if os.path.exists(cookie_path) and os.path.exists(profiles_file):
            with open(self.cookie_path, "w") as fh:
                yaml.dump(user, fh)

        return user

    def get_cookie(self):
        if not os.path.isfile(self.cookie_path):
            user = self.set_cookie()
        else:
            with open(self.cookie_path, "r") as fh:
                try:
                    user = safe_load(fh)
                    if user is None:
                        user = self.set_cookie()
                except yaml.reader.ReaderError:
                    user = self.set_cookie()
        return user


active_user: Optional[User] = None


def get_run_type(args):
    return "regular"


def get_invocation_context(user, config, args):
    # this adapter might not have implemented the type or unique_field properties
    try:
        adapter_type = config.credentials.type
    except Exception:
        adapter_type = None
    try:
        adapter_unique_id = config.credentials.hashed_unique_field()
    except Exception:
        adapter_unique_id = None

    return {
        "project_id": None if config is None else config.hashed_name(),
        "user_id": user.id,
        "invocation_id": get_invocation_id(),
        "command": args.which,
        "options": None,
        "version": str(dbt_version.installed),
        "run_type": get_run_type(args),
        "adapter_type": adapter_type,
        "adapter_unique_id": adapter_unique_id,
    }


def get_invocation_start_context(user, config, args):
    data = get_invocation_context(user, config, args)

    start_data = {"progress": "start", "result_type": None, "result": None}

    data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)


def get_invocation_end_context(user, config, args, result_type):
    data = get_invocation_context(user, config, args)

    start_data = {"progress": "end", "result_type": result_type, "result": None}

    data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)


def get_invocation_invalid_context(user, config, args, result_type):
    data = get_invocation_context(user, config, args)

    start_data = {"progress": "invalid", "result_type": result_type, "result": None}

    data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)


def get_platform_context():
    data = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_version": platform.python_implementation(),
    }

    return SelfDescribingJson(PLATFORM_SPEC, data)


def get_dbt_env_context():
    default = "manual"

    dbt_invocation_env = os.getenv(DBT_INVOCATION_ENV, default)
    if dbt_invocation_env == "":
        dbt_invocation_env = default

    data = {
        "environment": dbt_invocation_env,
    }

    return SelfDescribingJson(INVOCATION_ENV_SPEC, data)


def track(user, *args, **kwargs):
    if user.do_not_track:
        return
    else:
        fire_event(SendingEvent(kwargs=str(kwargs)))
        try:
            tracker.track_struct_event(*args, **kwargs)
        except Exception:
            fire_event(SendEventFailure())


def track_invocation_start(config=None, args=None):
    context = [
        get_invocation_start_context(active_user, config, args),
        get_platform_context(),
        get_dbt_env_context(),
    ]

    track(active_user, category="dbt", action="invocation", label="start", context=context)


def track_project_load(options):
    context = [SelfDescribingJson(LOAD_ALL_TIMING_SPEC, options)]
    assert active_user is not None, "Cannot track project loading time when active user is None"

    track(
        active_user,
        category="dbt",
        action="load_project",
        label=get_invocation_id(),
        context=context,
    )


def track_resource_counts(resource_counts):
    context = [SelfDescribingJson(RESOURCE_COUNTS, resource_counts)]
    assert active_user is not None, "Cannot track resource counts when active user is None"

    track(
        active_user,
        category="dbt",
        action="resource_counts",
        label=get_invocation_id(),
        context=context,
    )


def track_model_run(options):
    context = [SelfDescribingJson(RUN_MODEL_SPEC, options)]
    assert active_user is not None, "Cannot track model runs when active user is None"

    track(
        active_user, category="dbt", action="run_model", label=get_invocation_id(), context=context
    )


def track_rpc_request(options):
    context = [SelfDescribingJson(RPC_REQUEST_SPEC, options)]
    assert active_user is not None, "Cannot track rpc requests when active user is None"

    track(
        active_user,
        category="dbt",
        action="rpc_request",
        label=get_invocation_id(),
        context=context,
    )


def track_package_install(config, args, options):
    assert active_user is not None, "Cannot track package installs when active user is None"

    invocation_data = get_invocation_context(active_user, config, args)

    context = [
        SelfDescribingJson(INVOCATION_SPEC, invocation_data),
        SelfDescribingJson(PACKAGE_INSTALL_SPEC, options),
    ]

    track(
        active_user,
        category="dbt",
        action="package",
        label=get_invocation_id(),
        property_="install",
        context=context,
    )


def track_deprecation_warn(options):

    assert active_user is not None, "Cannot track deprecation warnings when active user is None"

    context = [SelfDescribingJson(DEPRECATION_WARN_SPEC, options)]

    track(
        active_user,
        category="dbt",
        action="deprecation",
        label=get_invocation_id(),
        property_="warn",
        context=context,
    )


def track_invocation_end(config=None, args=None, result_type=None):
    user = active_user
    context = [
        get_invocation_end_context(user, config, args, result_type),
        get_platform_context(),
        get_dbt_env_context(),
    ]

    assert active_user is not None, "Cannot track invocation end when active user is None"

    track(active_user, category="dbt", action="invocation", label="end", context=context)


def track_invalid_invocation(config=None, args=None, result_type=None):
    assert active_user is not None, "Cannot track invalid invocations when active user is None"

    user = active_user
    invocation_context = get_invocation_invalid_context(user, config, args, result_type)

    context = [invocation_context, get_platform_context(), get_dbt_env_context()]

    track(active_user, category="dbt", action="invocation", label="invalid", context=context)


def track_experimental_parser_sample(options):
    context = [SelfDescribingJson(EXPERIMENTAL_PARSER, options)]
    assert (
        active_user is not None
    ), "Cannot track experimental parser info when active user is None"

    track(
        active_user,
        category="dbt",
        action="experimental_parser",
        label=get_invocation_id(),
        context=context,
    )


def track_partial_parser(options):
    context = [SelfDescribingJson(PARTIAL_PARSER, options)]
    assert active_user is not None, "Cannot track partial parser info when active user is None"

    track(
        active_user,
        category="dbt",
        action="partial_parser",
        label=get_invocation_id(),
        context=context,
    )


def track_runnable_timing(options):
    context = [SelfDescribingJson(RUNNABLE_TIMING, options)]
    assert active_user is not None, "Cannot track runnable info when active user is None"
    track(
        active_user,
        category="dbt",
        action="runnable_timing",
        label=get_invocation_id(),
        context=context,
    )


def flush():
    fire_event(FlushEvents())
    try:
        tracker.flush()
    except Exception:
        fire_event(FlushEventsFailure())


def disable_tracking():
    global active_user
    if active_user is not None:
        active_user.disable_tracking()
    else:
        active_user = User(None)


def do_not_track():
    global active_user
    active_user = User(None)


def initialize_tracking(cookie_dir):
    global active_user
    active_user = User(cookie_dir)
    try:
        active_user.initialize()
    except Exception:
        fire_event(TrackingInitializeFailure())
        active_user = User(None)


class InvocationProcessor(logbook.Processor):
    def __init__(self):
        super().__init__()

    def process(self, record):
        if active_user is not None:
            record.extra.update(
                {
                    "run_started_at": active_user.run_started_at.isoformat(),
                    "invocation_id": get_invocation_id(),
                }
            )


def initialize_from_flags():
    # Setting these used to be in UserConfig, but had to be moved here
    if flags.SEND_ANONYMOUS_USAGE_STATS:
        initialize_tracking(flags.PROFILES_DIR)
    else:
        do_not_track()
