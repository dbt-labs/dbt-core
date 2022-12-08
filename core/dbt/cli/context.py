from click import Context, Group, Command
from click.exceptions import NoSuchOption, UsageError
from dbt.cli.flags import Flags
from dbt.config.project import Project
import sys


class DBTUsageException(Exception):
    pass


class DBTContext(Context):
    def __init__(self, command: Command, **kwargs) -> None:
        if isinstance(kwargs.get("parent"), DBTContext):
            self.invocation_args = kwargs["parent"].invocation_args
        else:
            self.invocation_args = kwargs.pop("args", sys.argv[1:])

        super().__init__(command, **kwargs)

        # Bubble up validation errors for top-level commands
        if not self.parent:
            self._validate_args(command, self.invocation_args)

        self.obj = self.obj or {}
        self.flags = Flags(self)

    def _validate_args(self, command, args) -> None:
        try:
            command.parse_args(self, args)
            if isinstance(command, Group):
                _, cmd, cmd_args = command.resolve_command(self, args)
                self._validate_args(cmd, cmd_args)
        except (NoSuchOption, UsageError) as e:
            raise DBTUsageException(e.message)

    def set_project(self, project: Project):
        if not isinstance(project, Project):
            raise ValueError(f"{project} is a {type(project)}, expected a Project object.")

        self.obj["project"] = project
