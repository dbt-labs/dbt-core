import operator
from datetime import datetime, timedelta
from typing import Optional, List, Iterable, Tuple

import dbt.exceptions
from dbt.contracts.rpc import (
    GCSettings,
    GCResultState,
    GCResult,
    TaskID,
)
from dbt.rpc.task_handler_protocol import TaskHandlerMap

# import this to make sure our timedelta encoder is registered
from dbt import helper_types  # noqa


class GarbageCollector:
    def __init__(
        self,
        active_tasks: TaskHandlerMap,
        settings: Optional[GCSettings] = None,
    ) -> None:
        self.active_tasks: TaskHandlerMap = active_tasks
        self.settings: GCSettings

        if settings is None:
            self.settings = GCSettings(
                maxsize=1000, reapsize=500, auto_reap_age=timedelta(days=30)
            )
        else:
            self.settings = settings

    def _remove_task_if_finished(self, task_id: TaskID) -> GCResultState:
        """Remove the task if it was finished. Raises a KeyError if the entry
        is removed during operation (so hold the lock).
        """
        if task_id not in self.active_tasks:
            return GCResultState.Missing

        task = self.active_tasks[task_id]
        if not task.state.finished:
            return GCResultState.Running

        del self.active_tasks[task_id]
        return GCResultState.Deleted

    def _get_before_list(self, when: datetime) -> List[TaskID]:
        removals: List[TaskID] = []
        for task in self.active_tasks.values():
            if not task.state.finished:
                continue
            elif task.ended is None:
                continue
            elif task.ended < when:
                removals.append(task.task_id)

        return removals

    def _get_oldest_ended_list(self, num: int) -> List[TaskID]:
        candidates: List[Tuple[datetime, TaskID]] = []
        for task in self.active_tasks.values():
            if not task.state.finished:
                continue
            elif task.ended is None:
                continue
            else:
                candidates.append((task.ended, task.task_id))
        candidates.sort(key=operator.itemgetter(0))
        return [task_id for _, task_id in candidates[:num]]

    def collect_task_id(
        self, result: GCResult, task_id: TaskID
    ) -> None:
        """To collect a task ID, we just delete it from the tasks dict.

        You must hold the lock, as this mutates `tasks`.
        """
        try:
            state = self._remove_task_if_finished(task_id)
        except KeyError:
            # someone was mutating tasks while we had the lock, that's
            # not right!
            raise dbt.exceptions.InternalException(
                'Got a KeyError for task uuid={} during gc'
                .format(task_id)
            )

        return result.add_result(task_id=task_id, state=state)

    def collect_multiple_task_ids(
        self, task_ids: Iterable[TaskID]
    ) -> GCResult:
        result = GCResult()
        for task_id in task_ids:
            self.collect_task_id(result, task_id)
        return result

    def collect_as_required(self) -> None:
        to_remove: List[TaskID] = []
        num_tasks = len(self.active_tasks)
        if num_tasks > self.settings.maxsize:
            num = self.settings.maxsize - num_tasks
            to_remove = self._get_oldest_ended_list(num)
        elif num_tasks > self.settings.reapsize:
            before = datetime.utcnow() - self.settings.auto_reap_age
            to_remove = self._get_before_list(before)

        if to_remove:
            self.collect_multiple_task_ids(to_remove)

    def collect_selected(
        self,
        task_ids: Optional[List[TaskID]] = None,
        before: Optional[datetime] = None,
        settings: Optional[GCSettings] = None,
    ) -> GCResult:
        to_gc = set()

        if task_ids is not None:
            to_gc.update(task_ids)
        if settings:
            self.settings = settings
        # we need the lock for this!
        if before is not None:
            to_gc.update(self._get_before_list(before))
        return self.collect_multiple_task_ids(to_gc)
