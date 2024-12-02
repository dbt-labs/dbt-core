from dbt.runner.no_op_runner import NoOpRunner


class SavedQueryRunner(NoOpRunner):
    @property
    def description(self) -> str:
        return f"saved query {self.node.name}"
