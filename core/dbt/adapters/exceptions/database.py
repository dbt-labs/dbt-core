from typing import Any

from dbt.common.exceptions import NotImplementedError, CompilationError


class UnexpectedDbReferenceError(NotImplementedError):
    def __init__(self, adapter, database, expected):
        self.adapter = adapter
        self.database = database
        self.expected = expected
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f"Cross-db references not allowed in {self.adapter} ({self.database} vs {self.expected})"
        return msg


class CrossDbReferenceProhibitedError(CompilationError):
    def __init__(self, adapter, exc_msg: str):
        self.adapter = adapter
        self.exc_msg = exc_msg
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f"Cross-db references not allowed in adapter {self.adapter}: Got {self.exc_msg}"
        return msg


class IndexConfigNotDictError(CompilationError):
    def __init__(self, raw_index: Any):
        self.raw_index = raw_index
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f"Invalid index config:\n"
            f"  Got: {self.raw_index}\n"
            f'  Expected a dictionary with at minimum a "columns" key'
        )
        return msg


class IndexConfigError(CompilationError):
    def __init__(self, exc: TypeError):
        self.exc = exc
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        validator_msg = self.validator_error_message(self.exc)
        msg = f"Could not parse index config: {validator_msg}"
        return msg
