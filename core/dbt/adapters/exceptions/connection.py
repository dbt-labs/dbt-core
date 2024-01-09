from typing import List

from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError


class InvalidConnectionError(DbtRuntimeError):
    def __init__(self, thread_id, known: List) -> None:
        self.thread_id = thread_id
        self.known = known
        super().__init__(
            msg=f"connection never acquired for thread {self.thread_id}, have {self.known}"
        )


class FailedToConnectError(DbtDatabaseError):
    pass
