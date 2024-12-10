from dbt.runners.no_op_runner import NoOpRunner


class ExposureRunner(NoOpRunner):
    @property
    def description(self) -> str:
        return f"exposure {self.node.name}"
