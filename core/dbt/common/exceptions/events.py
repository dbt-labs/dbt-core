from dbt.common.exceptions import CompilationError, scrub_secrets, env_secrets


# event level exception
class EventCompilationError(CompilationError):
    def __init__(self, msg: str, node) -> None:
        self.msg = scrub_secrets(msg, env_secrets())
        self.node = node
        super().__init__(msg=self.msg)
