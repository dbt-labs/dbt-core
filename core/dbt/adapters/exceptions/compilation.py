from dbt.common.exceptions import CompilationError


class MissingConfigError(CompilationError):
    def __init__(self, unique_id: str, name: str):
        self.unique_id = unique_id
        self.name = name
        msg = (
            f"Model '{self.unique_id}' does not define a required config parameter '{self.name}'."
        )
        super().__init__(msg=msg)
