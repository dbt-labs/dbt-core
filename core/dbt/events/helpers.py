import os
from typing import List
from dbt.constants import SECRET_ENV_PREFIX
from datetime import datetime


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = str(msg)

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


# This converts a datetime to a json format datetime string which
# is used in constructing protobuf message timestamps.
def datetime_to_json_string(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# preformatted time stamp
def get_json_string_utcnow() -> str:
    ts = datetime.utcnow()
    ts_rfc3339 = datetime_to_json_string(ts)
    return ts_rfc3339
