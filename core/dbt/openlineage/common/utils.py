import datetime
import re
from typing import Any, Dict, List, Optional

from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)

from dbt.cli.flags import Flags
from dbt.cli.types import Command
from dbt_common.events.base_types import EventMsg


def get_attribute(e: EventMsg, attribute: str, default: Any = None) -> Any:
    attributes = attribute.split(".")
    current = e
    for attr in attributes:
        if hasattr(current, attr):
            current = getattr(current, attr)
        else:
            return default
    return current


def get_event_time(seconds: int) -> str:
    return datetime.datetime.fromtimestamp(seconds).isoformat()


def generate_run_event(
    event_type: RunState,
    event_time: str,
    run_id: str,
    job_name: str,
    job_namespace: str,
    inputs: Optional[List[InputDataset]] = None,
    outputs: Optional[List[OutputDataset]] = None,
    job_facets: Optional[Dict] = None,
    run_facets: Optional[Dict] = None,
) -> RunEvent:
    inputs = inputs or []
    outputs = outputs or []
    job_facets = job_facets or {}
    run_facets = run_facets or {}
    return RunEvent(
        eventType=event_type,
        eventTime=event_time,
        run=Run(runId=run_id, facets=run_facets),
        job=Job(
            namespace=job_namespace,
            name=job_name,
            facets=job_facets,
        ),
        inputs=inputs,
        outputs=outputs,
        producer=get_openlineage_producer(),
    )


def get_openlineage_producer() -> str:
    version = _get_openlineage_version()
    return f"https://github.com/OpenLineage/OpenLineage/tree/{version}/integration/dbt"


def _get_openlineage_version() -> str:
    from importlib.metadata import version

    return version("openlineage-python")


def is_runnable_dbt_command(flags: Flags) -> bool:
    runnable_commands = {
        Command.RUN.value,
        Command.BUILD.value,
        Command.TEST.value,
        Command.SNAPSHOT.value,
        Command.SEED.value,
    }
    return flags.which in runnable_commands


def sanitize_job_name_component(s: str) -> str:
    """
    A utility function that sanitizes the job name component by replacing
    any non-alphanumeric characters with underscores.
    """
    return re.sub(r"[^a-zA-Z0-9_\-]", "__", s)
