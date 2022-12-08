from click import Context, Group, Command
from click.exceptions import NoSuchOption, UsageError
from dbt.config.runtime import load_project, load_profile
from dbt.cli.flags import Flags
import sys


class DBTUsageException(Exception):
    pass


class DBTContext(Context):
    def __init__(self, command: Command, **kwargs) -> None:
        invocation_args = kwargs.pop("args", sys.argv[1:])
        super().__init__(command, **kwargs)

        # Bubble up validation errors for top-level commands
        if not self.parent:
            self._validate_args(command, invocation_args)

        if not self.obj:
            flags = Flags(self, args=invocation_args)
            # TODO: fix flags.THREADS access
            # TODO: set accept pluggable profile, project objects
            profile = load_profile(flags.PROJECT_DIR, flags.VARS, flags.PROFILE, flags.TARGET, None)  # type: ignore
            project = load_project(flags.PROJECT_DIR, flags.VERSION_CHECK, profile, flags.VARS)  # type: ignore
            self.obj = {}
            self.obj["flags"] = flags
            self.obj["profile"] = profile
            self.obj["project"] = project

    def _validate_args(self, command, args) -> None:
        try:
            command.parse_args(self, args)
            if isinstance(command, Group):
                _, cmd, cmd_args = command.resolve_command(self, args)
                self._validate_args(cmd, cmd_args)
        except (NoSuchOption, UsageError) as e:
            raise DBTUsageException(e.message)
