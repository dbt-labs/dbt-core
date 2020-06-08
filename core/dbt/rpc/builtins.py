import os
import signal
from datetime import datetime
from typing import Type, Union, Any, List, Dict

import dbt.exceptions
from dbt.contracts.rpc import (
    TaskTags,
    StatusParameters,
    LastParse,
    GCParameters,
    GCResult,
    GetManifestResult,
    KillParameters,
    KillResult,
    KillResultStatus,
    PSParameters,
    TaskRow,
    PSResult,
    RemoteExecutionResult,
    RemoteRunResult,
    RemoteCompileResult,
    RemoteCatalogResults,
    RemoteEmptyResult,
    RemoteRunOperationResult,
    PollParameters,
    PollResult,
    PollInProgressResult,
    PollKilledResult,
    PollExecuteCompleteResult,
    PollGetManifestResult,
    PollRunCompleteResult,
    PollCompileCompleteResult,
    PollCatalogCompleteResult,
    PollRemoteEmptyCompleteResult,
    PollRunOperationCompleteResult,
    TaskHandlerState,
    TaskTiming,
)
from dbt.logger import LogMessage
from dbt.rpc.error import dbt_error, RPCException
from dbt.rpc.method import RemoteBuiltinMethod
from dbt.rpc.task_handler import RequestTaskHandler


class GC(RemoteBuiltinMethod[GCParameters, GCResult]):
    METHOD_NAME = 'gc'

    def set_args(self, params: GCParameters):
        super().set_args(params)

    def handle_request(self) -> GCResult:
        if self.params is None:
            raise dbt.exceptions.InternalException('GC: params not set')
        return self.task_manager.gc_safe(
            task_ids=self.params.task_ids,
            before=self.params.before,
            settings=self.params.settings,
        )


class Kill(RemoteBuiltinMethod[KillParameters, KillResult]):
    METHOD_NAME = 'kill'

    def set_args(self, params: KillParameters):
        super().set_args(params)

    def handle_request(self) -> KillResult:
        if self.params is None:
            raise dbt.exceptions.InternalException('Kill: params not set')
        result = KillResult()
        task: RequestTaskHandler
        try:
            task = self.task_manager.get_request(self.params.task_id)
        except dbt.exceptions.UnknownAsyncIDException:
            # nothing to do!
            return result

        result.state = KillResultStatus.NotStarted

        if task.process is None:
            return result
        pid = task.process.pid
        if pid is None:
            return result

        if task.process.is_alive():
            result.state = KillResultStatus.Killed
            task.ended = datetime.utcnow()
            os.kill(pid, signal.SIGINT)
            task.state = TaskHandlerState.Killed
        else:
            result.state = KillResultStatus.Finished
            # the state must be "Completed"

        return result


class Status(RemoteBuiltinMethod[StatusParameters, LastParse]):
    METHOD_NAME = 'status'

    def set_args(self, params: StatusParameters):
        super().set_args(params)

    def handle_request(self) -> LastParse:
        return self.task_manager.last_parse


class PS(RemoteBuiltinMethod[PSParameters, PSResult]):
    METHOD_NAME = 'ps'

    def set_args(self, params: PSParameters):
        super().set_args(params)

    def keep(self, row: TaskRow):
        if self.params is None:
            raise dbt.exceptions.InternalException('PS: params not set')
        if row.state.finished and self.params.completed:
            return True
        elif not row.state.finished and self.params.active:
            return True
        else:
            return False

    def handle_request(self) -> PSResult:
        rows = [
            row for row in self.task_manager.task_table() if self.keep(row)
        ]
        rows.sort(key=lambda r: (r.state, r.start, r.method))
        result = PSResult(rows=rows, logs=[])
        return result


def poll_complete(
    timing: TaskTiming, result: Any, tags: TaskTags, logs: List[LogMessage]
) -> PollResult:
    if timing.state not in (TaskHandlerState.Success, TaskHandlerState.Failed):
        raise dbt.exceptions.InternalException(
            f'got invalid result state in poll_complete: {timing.state}'
        )

    cls: Type[Union[
        PollExecuteCompleteResult,
        PollRunCompleteResult,
        PollCompileCompleteResult,
        PollCatalogCompleteResult,
        PollRemoteEmptyCompleteResult,
        PollRunOperationCompleteResult,
        PollGetManifestResult
    ]]

    if isinstance(result, RemoteExecutionResult):
        cls = PollExecuteCompleteResult
    # order matters here, as RemoteRunResult subclasses RemoteCompileResult
    elif isinstance(result, RemoteRunResult):
        cls = PollRunCompleteResult
    elif isinstance(result, RemoteCompileResult):
        cls = PollCompileCompleteResult
    elif isinstance(result, RemoteCatalogResults):
        cls = PollCatalogCompleteResult
    elif isinstance(result, RemoteEmptyResult):
        cls = PollRemoteEmptyCompleteResult
    elif isinstance(result, RemoteRunOperationResult):
        cls = PollRunOperationCompleteResult
    elif isinstance(result, GetManifestResult):
        cls = PollGetManifestResult
    else:
        raise dbt.exceptions.InternalException(
            'got invalid result in poll_complete: {}'.format(result)
        )
    return cls.from_result(result, tags, timing, logs)


def _dict_logs(logs: List[LogMessage]) -> List[Dict[str, Any]]:
    return [log.to_dict() for log in logs]


class Poll(RemoteBuiltinMethod[PollParameters, PollResult]):
    METHOD_NAME = 'poll'

    def set_args(self, params: PollParameters):
        super().set_args(params)

    def handle_request(self) -> PollResult:
        if self.params is None:
            raise dbt.exceptions.InternalException('Poll: params not set')
        task_id = self.params.request_token
        task: RequestTaskHandler = self.task_manager.get_request(task_id)

        task_logs: List[LogMessage] = []
        if self.params.logs:
            task_logs = task.logs[self.params.logs_start:]

        # Get a state and store it locally so we ignore updates to state,
        # otherwise things will get confusing. States should always be
        # "forward-compatible" so if the state has transitioned to error/result
        # but we aren't there yet, the logs will still be valid.

        timing = task.make_task_timing(datetime.utcnow())
        state = timing.state
        if state <= TaskHandlerState.Running:
            return PollInProgressResult(
                tags=task.tags,
                logs=task_logs,
                state=timing.state,
                start=timing.start,
                end=timing.end,
                elapsed=timing.elapsed,
            )
        elif state == TaskHandlerState.Error:
            err = task.error
            if err is None:
                exc = dbt.exceptions.InternalException(
                    f'At end of task {task_id}, error state but error is None'
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=_dict_logs(task_logs))
                )
            # the exception has logs already attached from the child, don't
            # overwrite those
            raise err
        elif state in (TaskHandlerState.Success, TaskHandlerState.Failed):

            if task.result is None:
                exc = dbt.exceptions.InternalException(
                    f'At end of task {task_id}, state={state} but result is '
                    'None'
                )
                raise RPCException.from_error(
                    dbt_error(exc, logs=_dict_logs(task_logs))
                )
            return poll_complete(
                timing=timing,
                result=task.result,
                tags=task.tags,
                logs=task_logs
            )
        elif state == TaskHandlerState.Killed:
            return PollKilledResult(
                tags=task.tags,
                logs=task_logs,
                state=timing.state,
                start=timing.start,
                end=timing.end,
                elapsed=timing.elapsed,
            )
        else:
            exc = dbt.exceptions.InternalException(
                f'Got unknown value state={state} for task {task_id}'
            )
            raise RPCException.from_error(
                dbt_error(exc, logs=_dict_logs(task_logs))
            )
