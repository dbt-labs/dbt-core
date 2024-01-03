from typing import Any
from dbt.common.exceptions import CompilationError


# this is part of the context and also raised in dbt.contracts.relation.py
class DataclassNotDictError(CompilationError):
    def __init__(self, obj: Any):
        self.obj = obj
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f'The object ("{self.obj}") was used as a dictionary. This '
            "capability has been removed from objects of this type."
        )

        return msg
