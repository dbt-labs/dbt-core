import enum
import os
from typing import Dict, Optional

import attr
from click import Context
from openlineage.client.facet_v2 import (
    BaseFacet,
    error_message_run,
    job_type_job,
    parent_run,
    sql_job,
)

from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.resources.types import NodeType
from dbt.events.types import CommandCompleted, FoundStats
from dbt.openlineage.common.utils import (
    get_attribute,
    get_openlineage_producer,
    sanitize_job_name_component,
)
from dbt_common.events.base_types import EventMsg

GITHUB_LOCATION = (
    "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/schema/"
)
OPENLINEAGE_DBT_JOB_NAME_ENV_VAR = "OPENLINEAGE_DBT_JOB_NAME"
OPENLINEAGE_NAMESPACE_ENV_VAR = "OPENLINEAGE_NAMESPACE"
OPENLINEAGE_PARENT_ID_ENV_VAR = "OPENLINEAGE_PARENT_ID"


@attr.s
class DbtVersionRunFacet(BaseFacet):
    version: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-version-run-facet.json"


@attr.s
class ParentRunMetadata:
    run_id: str = attr.ib()
    job_name: str = attr.ib()
    job_namespace: str = attr.ib()

    def to_openlineage(self) -> parent_run.ParentRunFacet:
        return parent_run.ParentRunFacet(
            run=parent_run.Run(runId=self.run_id),
            job=parent_run.Job(namespace=self.job_namespace, name=self.job_name),
        )


class DbtOpenlineageJobType(enum.Enum):
    job = "JOB"
    model = "MODEL"
    seed = "SEED"
    snapshot = "SNAPSHOT"
    test = "TEST"
    sql = "SQL"


def get_parent_run_facet() -> Dict:
    # The parent job that started the dbt command. Usually the scheduler (Airflow, ...etc)
    parent_id = os.getenv(OPENLINEAGE_PARENT_ID_ENV_VAR)
    parent_run_facet = {}
    if parent_id:
        parent_namespace, parent_job_name, parent_run_id = parent_id.split("/")
        parent_run_facet["parent"] = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace,
        ).to_openlineage()
    return parent_run_facet


def get_dbt_command_parent_run_facet(parent_run_metadata: ParentRunMetadata) -> Dict:
    return {"parent": parent_run_metadata.to_openlineage()}


def get_sql_job_facet(e: SQLQuery) -> Dict:
    return {"sql": sql_job.SQLJobFacet(query=e.data.sql)}


def get_error_message_run_facet(error_message: str, stacktrace=None) -> Dict:
    return {
        "errorMessage": error_message_run.ErrorMessageRunFacet(
            message=error_message, programmingLanguage="sql", stackTrace=stacktrace
        )
    }


def get_dbt_version_facet() -> Dict:
    from dbt.version import __version__ as dbt_version

    return {"dbt_version": DbtVersionRunFacet(version=dbt_version)}


def get_job_type_facet(e: EventMsg) -> Dict:
    job_type = _get_job_type(e)
    return {
        "jobType": job_type_job.JobTypeJobFacet(
            jobType=job_type,
            integration="DBT",
            processingType="BATCH",
            producer=get_openlineage_producer(),
        )
    }


def _get_job_type(e: EventMsg) -> Optional[str]:
    node_resource_type = get_attribute(e, "data.node_info.resource_type")
    event_name = get_attribute(e, "info.name")
    if event_name == SQLQuery.__name__:
        return DbtOpenlineageJobType.sql.value
    elif event_name in (CommandCompleted.__name__, FoundStats.__name__):
        return DbtOpenlineageJobType.job.value
    elif node_resource_type == NodeType.Model:
        return DbtOpenlineageJobType.model.value
    elif node_resource_type == NodeType.Snapshot:
        return DbtOpenlineageJobType.snapshot.value
    elif node_resource_type == NodeType.Seed:
        return DbtOpenlineageJobType.seed.value
    elif node_resource_type == NodeType.Test:
        return DbtOpenlineageJobType.test.value
    return None


def get_job_name(e: EventMsg, ctx: Context) -> str:
    """
    Openlineage job_name is computed, in the following order:
    1. using an environment variable
    2. using the selectors
    3. using a default value
    """
    if e.info.name in (CommandCompleted.__name__, FoundStats.__name__):
        project_name = ctx.obj["project"].project_name
        dbt_flags = ctx.obj["flags"]
        profile = ctx.obj["profile"]
        default_job_name = f"dbt-run--project={project_name}--profile={profile.profile_name}"

        dbt_job_name_env = os.environ.get(OPENLINEAGE_DBT_JOB_NAME_ENV_VAR)

        selector = dbt_flags.selector
        select = dbt_flags.select
        exclude = dbt_flags.exclude

        if dbt_job_name_env:
            return dbt_job_name_env
        elif select or exclude:
            selected_models = "-".join(sanitize_job_name_component(model) for model in select)
            job_name = f"{default_job_name}--select={selected_models}"
            if exclude:
                excluded_models = "-".join(sanitize_job_name_component(model) for model in exclude)
                job_name = f"{job_name}--exclude={excluded_models}"
            return job_name
        elif selector:
            job_name = f"{default_job_name}--selector={sanitize_job_name_component(selector)}"
            return job_name
        else:
            return default_job_name
    else:
        return get_attribute(e, "data.node_info.unique_id") or "dbt-run"


def get_job_namespace() -> str:
    return os.environ.get(OPENLINEAGE_NAMESPACE_ENV_VAR, "dbt")
