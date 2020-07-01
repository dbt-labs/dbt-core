import multiprocessing
from datetime import datetime
from typing import Optional, Union, MutableMapping
from typing_extensions import Protocol

import dbt.exceptions
from dbt.contracts.rpc import (
    TaskHandlerState,
    TaskID,
    TaskTags,
    TaskTiming,
    TaskRow,
)


class TaskHandlerProtocol(Protocol):
    started: Optional[datetime]
    ended: Optional[datetime]
    state: TaskHandlerState
    task_id: TaskID
    process: Optional[multiprocessing.Process]

    @property
    def request_id(self) -> Union[str, int]:
        pass

    @property
    def request_source(self) -> str:
        pass

    @property
    def timeout(self) -> Optional[float]:
        pass

    @property
    def method(self) -> str:
        pass

    @property
    def tags(self) -> Optional[TaskTags]:
        pass

    def _assert_started(self) -> datetime:
        if self.started is None:
            raise dbt.exceptions.InternalException(
                'task handler started but start time is not set'
            )
        return self.started

    def _assert_ended(self) -> datetime:
        if self.ended is None:
            raise dbt.exceptions.InternalException(
                'task handler finished but end time is not set'
            )
        return self.ended

    def make_task_timing(
        self, now_time: datetime
    ) -> TaskTiming:
        # get information about the task in a way that should not provide any
        # conflicting information. Calculate elapsed time based on `now_time`
        state = self.state
        # store end/start so 'ps' output always makes sense:
        # not started -> no start time/elapsed, running -> no end time, etc
        end = None
        start = None
        elapsed = None
        if state > TaskHandlerState.NotStarted:
            start = self._assert_started()
            elapsed_end = now_time

            if state.finished:
                elapsed_end = self._assert_ended()
                end = elapsed_end

            elapsed = (elapsed_end - start).total_seconds()
        return TaskTiming(state=state, start=start, end=end, elapsed=elapsed)

    def make_task_row(self, now_time: datetime) -> TaskRow:
        timing = self.make_task_timing(now_time)

        return TaskRow(
            task_id=self.task_id,
            request_id=self.request_id,
            request_source=self.request_source,
            method=self.method,
            state=timing.state,
            start=timing.start,
            end=timing.end,
            elapsed=timing.elapsed,
            timeout=self.timeout,
            tags=self.tags,
        )


TaskHandlerMap = MutableMapping[TaskID, TaskHandlerProtocol]
