from typing import List
import os

from dbt.common.constants import SECRET_ENV_PREFIX


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = str(msg)

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed
