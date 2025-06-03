import traceback
from collections import defaultdict
from typing import Dict, List, Optional, Union

from click import Context
from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.facet_v2 import data_quality_assertions_dataset
from openlineage.client.uuid import generate_new_uuid

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.schemas.results import NodeStatus
from dbt.cli.flags import Flags
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import GenericTestNode
from dbt.events.types import (
    CatchableExceptionOnRun,
    CommandCompleted,
    FoundStats,
    NodeFinished,
    NodeStart,
    OpenLineageException,
)
from dbt.openlineage.common.dataset_facets import (
    extract_namespace,
    get_model_inputs,
    get_test_column,
    node_to_dataset,
)
from dbt.openlineage.common.facets import (
    ParentRunMetadata,
    get_dbt_command_parent_run_facet,
    get_dbt_version_facet,
    get_error_message_run_facet,
    get_job_name,
    get_job_namespace,
    get_job_type_facet,
    get_parent_run_facet,
    get_sql_job_facet,
)
from dbt.openlineage.common.utils import (
    generate_run_event,
    get_attribute,
    get_event_time,
)
from dbt_common.events.base_types import EventMsg
from dbt_common.events.functions import fire_event


class OpenLineageHandler:

    def __init__(self, ctx: Context):
        self.dbt_command_metadata: ParentRunMetadata

        self.client = OpenLineageClient()
        self.ctx = ctx
        self.node_id_to_ol_run_id: Dict[str, str] = defaultdict(lambda: str(generate_new_uuid()))

        # sql query ids are incremented sequentially per node_id
        self.node_id_to_sql_query_id: Dict[str, int] = defaultdict(lambda: 1)
        self.node_id_to_sql_start_event: Dict[str, RunEvent] = {}
        self.node_id_to_node_start_event: Dict[str, RunEvent] = {}

    @property
    def _flags(self) -> Flags:
        return self.ctx.obj["flags"]

    @property
    def _adapter_credentials(self) -> Credentials:
        return self.ctx.obj["runtime_config"].credentials

    @property
    def _manifest(self) -> Manifest:
        return self.ctx.obj["manifest"]

    def handle(self, e: EventMsg):
        """
        Callback passed to the eventManager.
        All exceptions are handled in this function. This callback never makes dbt fail.
        """
        if e.info.name == OpenLineageException.__name__:
            return
        try:
            self.handle_unsafe(e)
        except Exception as exception:
            self._handle_exception(exception)

    def _handle_exception(self, e: Exception):
        fire_event(OpenLineageException(exc=str(e), exc_info=traceback.format_exc()))

    def handle_unsafe(self, e: EventMsg):
        event_name = e.info.name
        openlineage_event = None

        if event_name == FoundStats.__name__:
            openlineage_event = self._parse_dbt_start_command_event(e, self.ctx)  # type: ignore[arg-type]
            self._setup_dbt_command_metadata(openlineage_event, self.ctx)

        elif event_name == CommandCompleted.__name__:
            openlineage_event = self._parse_command_completed_event(e, self.ctx)  # type: ignore[arg-type]

        node_unique_id = get_attribute(e, "data.node_info.unique_id")

        if node_unique_id:
            if event_name == NodeStart.__name__:
                openlineage_event = self._parse_node_start_event(e, self.ctx)  # type: ignore[arg-type]
            elif event_name == SQLQuery.__name__:
                openlineage_event = self._parse_sql_query_event(e, self.ctx)  # type: ignore[arg-type]
            elif event_name in (SQLQueryStatus.__name__, CatchableExceptionOnRun.__name__):
                openlineage_event = self._parse_sql_query_status_event(e, self.ctx)  # type: ignore[arg-type]
            elif event_name == NodeFinished.__name__:
                openlineage_event = self.parse_node_finished_event(e, self.ctx)  # type: ignore[arg-type]

        if openlineage_event:
            self.emit(openlineage_event)

    def emit(self, openlineage_event: RunEvent):
        self.client.emit(openlineage_event)

    def _parse_dbt_start_command_event(self, e: FoundStats, ctx: Context) -> RunEvent:
        event_time = get_event_time(e.info.ts.seconds)
        parent_run_facet = get_parent_run_facet()
        dbt_version_facet = get_dbt_version_facet()
        run_facets = {**parent_run_facet, **dbt_version_facet}

        job_type_facet = get_job_type_facet(e)

        event_run_id = str(generate_new_uuid())

        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=event_time,
            run_id=event_run_id,
            job_name=get_job_name(e, ctx),
            job_namespace=get_job_namespace(),
            run_facets=run_facets,
            job_facets=job_type_facet,
        )
        return start_event

    def _parse_command_completed_event(self, e: CommandCompleted, ctx: Context) -> RunEvent:
        success = e.data.success
        event_time = get_event_time(e.data.completed_at.seconds)
        dbt_version_facet = get_dbt_version_facet()
        parent_run_facet = get_parent_run_facet()
        error_message_run_facet = {}
        if success:
            run_state = RunState.COMPLETE
        else:
            run_state = RunState.FAIL
            error_message_run_facet = get_error_message_run_facet(e.info.msg)

        run_facets = {**parent_run_facet, **dbt_version_facet, **error_message_run_facet}
        job_type_facet = get_job_type_facet(e)

        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=self.dbt_command_metadata.run_id,
            job_name=self.dbt_command_metadata.job_name,
            job_namespace=self.dbt_command_metadata.job_namespace,
            job_facets={**job_type_facet},
            run_facets=run_facets,
        )

    def _setup_dbt_command_metadata(self, start_event: RunEvent, ctx: Context):
        self.dbt_command_metadata = ParentRunMetadata(
            run_id=start_event.run.runId,
            job_name=start_event.job.name,
            job_namespace=start_event.job.namespace,
        )

    def _parse_node_start_event(self, e: NodeStart, ctx: Context) -> RunEvent:
        node_unique_id = e.data.node_info.unique_id
        node_start_time = e.data.node_info.node_started_at

        run_id = self.node_id_to_ol_run_id[node_unique_id]
        parent_run_facet = get_dbt_command_parent_run_facet(self.dbt_command_metadata)
        dbt_version_facet = get_dbt_version_facet()

        job_name = get_job_name(e, ctx)
        job_type_facet = get_job_type_facet(e)

        dataset_namespace = extract_namespace(self._adapter_credentials)
        inputs = []
        for input in get_model_inputs(node_unique_id, self._manifest):
            dataset = node_to_dataset(input, dataset_namespace)
            inputs.append(
                InputDataset(
                    namespace=dataset.namespace,
                    name=dataset.name,
                    facets=dataset.facets,
                )
            )

        output_dataset = node_to_dataset(self._manifest.nodes[node_unique_id], dataset_namespace)
        outputs = [
            OutputDataset(
                namespace=output_dataset.namespace,
                name=output_dataset.name,
                facets=output_dataset.facets,
            )
        ]

        run_facets = {**parent_run_facet, **dbt_version_facet}
        run_event = generate_run_event(
            event_type=RunState.START,
            event_time=node_start_time,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=get_job_namespace(),
            job_facets=job_type_facet,
            inputs=inputs,
            outputs=outputs,
        )

        self.node_id_to_node_start_event[node_unique_id] = run_event

        return run_event

    def parse_node_finished_event(self, e: NodeFinished, ctx: Context) -> RunEvent:
        resource_type = e.data.node_info.resource_type
        node_unique_id = e.data.node_info.unique_id
        node_finished_time = e.data.node_info.node_finished_at or e.data.node_info.node_started_at
        node_status = e.data.run_result.status
        run_id = self.node_id_to_ol_run_id[node_unique_id]
        parent_run_facet = get_dbt_command_parent_run_facet(self.dbt_command_metadata)
        dbt_version_facet = get_dbt_version_facet()

        job_name = get_job_name(e, ctx)
        job_type_facet = get_job_type_facet(e)
        event_type = None

        error_message_run_facet = {}

        if node_status == NodeStatus.Skipped:
            event_type = RunState.ABORT
        elif node_status in (NodeStatus.Fail, NodeStatus.Error, NodeStatus.RuntimeErr):
            event_type = RunState.FAIL
            error_message_run_facet = get_error_message_run_facet(e.data.run_result.message)
        elif node_status in (NodeStatus.Success, NodeStatus.Pass):
            event_type = RunState.COMPLETE
        else:
            event_type = RunState.OTHER

        inputs = self.node_id_to_node_start_event[node_unique_id].inputs
        outputs = self.node_id_to_node_start_event[node_unique_id].outputs

        run_facets = {**parent_run_facet, **dbt_version_facet, **error_message_run_facet}

        if resource_type == NodeType.Test:
            success = e.data.node_info.node_status == NodeStatus.Pass
            dataset_namespace = extract_namespace(self._adapter_credentials)
            input_dataset = self._get_test_input_dataset(
                node_unique_id, dataset_namespace, success
            )
            if input_dataset:
                inputs = [input_dataset]

        return generate_run_event(
            event_type=event_type,
            event_time=node_finished_time,
            run_id=run_id,
            run_facets=run_facets,
            job_name=job_name,
            job_namespace=get_job_namespace(),
            job_facets=job_type_facet,
            inputs=inputs,
            outputs=outputs,
        )

    def _parse_sql_query_event(self, e: SQLQuery, ctx: Context) -> RunEvent:
        node_unique_id = e.data.node_info.unique_id
        sql_ol_run_id = str(generate_new_uuid())
        sql_start_at = get_event_time(e.info.ts.seconds)

        parent_run_facet = self._get_dbt_sql_node_parent_run_facet(e)
        dbt_version_facet = get_dbt_version_facet()
        run_facets = {**parent_run_facet, **dbt_version_facet}

        job_type_facet = get_job_type_facet(e)
        sql_job_facet = get_sql_job_facet(e)
        job_facets = {**job_type_facet, **sql_job_facet}

        sql_event = generate_run_event(
            event_type=RunState.START,
            event_time=sql_start_at,
            run_id=sql_ol_run_id,
            job_name=self._get_sql_job_name(e),
            job_namespace=get_job_namespace(),
            run_facets=run_facets,
            job_facets=job_facets,
        )

        self.node_id_to_sql_start_event[node_unique_id] = sql_event

        return sql_event

    def _parse_sql_query_status_event(
        self, e: Union[SQLQueryStatus, CatchableExceptionOnRun], ctx: Context
    ) -> RunEvent:
        """
        If the sql query is successful a SQLQueryStatus is generated by dbt.
        In case of failure a CatchableExceptionOnRun is generated instead
        """
        node_unique_id = e.data.node_info.unique_id
        sql_ol_run_event = self.node_id_to_sql_start_event[node_unique_id]

        event_name = e.info.name
        run_state = RunState.OTHER
        event_time = get_event_time(e.info.ts.seconds)
        run_facets = {}
        if sql_ol_run_event.run.facets:
            run_facets = {k: v for k, v in sql_ol_run_event.run.facets.items()}

        if event_name == SQLQueryStatus.__name__:
            run_state = RunState.COMPLETE
        elif event_name == CatchableExceptionOnRun.__name__:
            run_state = RunState.FAIL
            error_message = e.data.exc
            stacktrace = e.data.exc_info
            run_facets.update(get_error_message_run_facet(error_message, stacktrace))

        return generate_run_event(
            event_type=run_state,
            event_time=event_time,
            run_id=sql_ol_run_event.run.runId,
            run_facets=run_facets,
            job_name=sql_ol_run_event.job.name,
            job_namespace=sql_ol_run_event.job.namespace,
            job_facets=sql_ol_run_event.job.facets,
        )

    def _get_sql_job_name(self, e: SQLQuery) -> str:
        """
        The name of the sql job is as follows
        {node_job_name}.sql.{incremental_id}
        """
        node_unique_id = e.data.node_info.unique_id
        query_id = self._get_sql_query_id(node_unique_id)
        job_name = f"{node_unique_id}.sql#{query_id}"

        return job_name

    def _get_data_quality_assertion_facet(
        self, test_node_id: str, success: bool
    ) -> Dict[str, data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet]:
        test_node = self._manifest.nodes[test_node_id]
        data_quality_assertion_facet = {}
        if isinstance(test_node, GenericTestNode):
            test_name = test_node.test_metadata.name
            column = get_test_column(test_node)
            assertion = data_quality_assertions_dataset.Assertion(
                assertion=test_name, success=success, column=column
            )
            data_quality_assertion_facet = {
                "dataQualityAssertions": data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                    assertions=[assertion]
                )
            }
        return data_quality_assertion_facet

    def _get_test_input_dataset(
        self, test_node_id: str, dataset_namespace: str, success: bool
    ) -> Optional[InputDataset]:
        test_node = self._manifest.nodes[test_node_id]
        input_dataset = None
        if isinstance(test_node, GenericTestNode):
            attached_node_id = test_node.attached_node
            if attached_node_id:
                assertion_facet = self._get_data_quality_assertion_facet(test_node_id, success)
                attached_node = self._manifest.nodes.get(
                    attached_node_id
                ) or self._manifest.sources.get(attached_node_id)
                fqn: List[str] = []
                if hasattr(attached_node, "fqn"):
                    fqn = attached_node.fqn  # type: ignore
                attached_dataset_name = ".".join(fqn)
                input_dataset = InputDataset(
                    namespace=dataset_namespace,
                    name=attached_dataset_name,
                    facets=assertion_facet,  # type: ignore
                )
        return input_dataset

    def _get_sql_query_id(self, node_id: str) -> int:
        """
        Not all adapters have the sql id defined in their dbt event.
        A node is executed by a single thread which means that sql queries of a single node are executed
        sequentially and their status is also reported sequentially.
        This function gives us an auto-incremented id for each sql query.
        Each sql query is associated with a node_id.
        """
        current_id = self.node_id_to_sql_query_id[node_id]
        self.node_id_to_sql_query_id[node_id] += 1
        return current_id

    def _get_dbt_sql_node_parent_run_facet(self, e: SQLQuery) -> Dict:
        node_unique_id = e.data.node_info.unique_id
        node_start_run_id = self.node_id_to_ol_run_id[node_unique_id]
        return {
            "parent": ParentRunMetadata(
                run_id=node_start_run_id,
                job_name=get_job_name(e, self.ctx),
                job_namespace=get_job_namespace(),
            ).to_openlineage()
        }
