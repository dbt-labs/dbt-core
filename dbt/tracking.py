from dbt.logger import GLOBAL_LOGGER as logger
from dbt import version as dbt_version
from snowplow_tracker import Subject, Tracker, Emitter, logger as sp_logger
from snowplow_tracker import SelfDescribingJson, disable_contracts
from datetime import datetime

import pytz
import platform
import uuid
import yaml
import os

import dbt.clients.system

disable_contracts()
sp_logger.setLevel(100)

COLLECTOR_URL = "fishtownanalytics.sinter-collect.com"
COLLECTOR_PROTOCOL = "https"

COOKIE_PATH = os.path.join(os.path.expanduser('~'), '.dbt/.user.yml')

INVOCATION_SPEC = 'iglu:com.dbt/invocation/jsonschema/1-0-0'
PLATFORM_SPEC = 'iglu:com.dbt/platform/jsonschema/1-0-0'
RUN_MODEL_SPEC = 'iglu:com.dbt/run_model/jsonschema/1-0-0'
INVOCATION_ENV_SPEC = 'iglu:com.dbt/invocation_env/jsonschema/1-0-0'
PACKAGE_INSTALL_SPEC = 'iglu:com.dbt/package_install/jsonschema/1-0-0'

DBT_INVOCATION_ENV = 'DBT_INVOCATION_ENV'

emitter = Emitter(COLLECTOR_URL, protocol=COLLECTOR_PROTOCOL, buffer_size=1)
tracker = Tracker(emitter, namespace="cf", app_id="dbt")

active_user = None


class User(object):

    def __init__(self):
        self.do_not_track = True

        self.id = None
        self.invocation_id = str(uuid.uuid4())
        self.run_started_at = datetime.now(tz=pytz.utc)

    def state(self):
        return "do not track" if self.do_not_track else "tracking"

    def initialize(self):
        self.do_not_track = False

        cookie = self.get_cookie()
        self.id = cookie.get('id')

        subject = Subject()
        subject.set_user_id(self.id)
        tracker.set_subject(subject)

    def set_cookie(self):
        cookie_dir = os.path.dirname(COOKIE_PATH)
        user = {"id": str(uuid.uuid4())}

        dbt.clients.system.make_directory(cookie_dir)

        with open(COOKIE_PATH, "w") as fh:
            yaml.dump(user, fh)

        return user

    def get_cookie(self):
        if not os.path.isfile(COOKIE_PATH):
            user = self.set_cookie()
        else:
            with open(COOKIE_PATH, "r") as fh:
                try:
                    user = yaml.safe_load(fh)
                    if user is None:
                        user = self.set_cookie()
                except yaml.reader.ReaderError:
                    user = self.set_cookie()
        return user


def get_run_type(args):
    return 'regular'


def get_invocation_context(user, project, args):
    return {
        "project_id": None if project is None else project.hashed_name(),
        "user_id": user.id,
        "invocation_id": user.invocation_id,

        "command": args.which,
        "options": None,
        "version": str(dbt_version.installed),

        "run_type": get_run_type(args),
    }


def get_invocation_start_context(user, project, args):
    data = get_invocation_context(user, project, args)

    start_data = {
        "progress": "start",
        "result_type": None,
        "result": None
    }

    data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)


def get_invocation_end_context(user, project, args, result_type):
    data = get_invocation_context(user, project, args)

    start_data = {
        "progress": "end",
        "result_type": result_type,
        "result": None
    }

    data.update(start_data)
    return SelfDescribingJson(INVOCATION_SPEC, data)


def get_invocation_invalid_context(user, project, args, result_type):
    data = get_invocation_context(user, project, args)

    start_data = {
        "progress": "invalid",
        "result_type": result_type,
        "result": None
    }

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
    default = 'manual'

    dbt_invocation_env = os.getenv(DBT_INVOCATION_ENV, default)
    if dbt_invocation_env == '':
        dbt_invocation_env = default

    data = {
        "environment": dbt_invocation_env,
    }

    return SelfDescribingJson(INVOCATION_ENV_SPEC, data)


def track(user, *args, **kwargs):
    if user.do_not_track:
        return
    else:
        logger.debug("Sending event: {}".format(kwargs))
        try:
            tracker.track_struct_event(*args, **kwargs)
        except Exception:
            logger.debug(
                "An error was encountered while trying to send an event"
            )


def track_invocation_start(project=None, args=None):
    context = [
        get_invocation_start_context(active_user, project, args),
        get_platform_context(),
        get_dbt_env_context()
    ]

    track(
        active_user,
        category="dbt",
        action='invocation',
        label='start',
        context=context
    )


def track_model_run(options):
    context = [SelfDescribingJson(RUN_MODEL_SPEC, options)]

    track(
        active_user,
        category="dbt",
        action='run_model',
        label=active_user.invocation_id,
        context=context
    )


def track_package_install(options):
    context = [SelfDescribingJson(PACKAGE_INSTALL_SPEC, options)]
    track(
        active_user,
        category="dbt",
        action='package',
        label=active_user.invocation_id,
        property_='install',
        context=context
    )


def track_invocation_end(
        project=None, args=None, result_type=None
):
    user = active_user
    context = [
        get_invocation_end_context(user, project, args, result_type),
        get_platform_context(),
        get_dbt_env_context()
    ]
    track(
        active_user,
        category="dbt",
        action='invocation',
        label='end',
        context=context
    )


def track_invalid_invocation(
        project=None, args=None, result_type=None
):

    user = active_user
    invocation_context = get_invocation_invalid_context(
        user,
        project,
        args,
        result_type
    )

    context = [
        invocation_context,
        get_platform_context(),
        get_dbt_env_context()
    ]

    track(
        active_user,
        category="dbt",
        action='invocation',
        label='invalid',
        context=context
    )


def flush():
    logger.debug("Flushing usage events")
    tracker.flush()


def do_not_track():
    global active_user
    active_user = User()


def initialize_tracking():
    global active_user
    active_user = User()
    active_user.initialize()
