from typing import List
from click import Context
from click.exceptions import NoSuchOption, UsageError
from dbt.cli.main import cli
from dbt.config.project import Project

class DBTUsageException(Exception):
    pass

class DBTContext(Context):
    def __init__(self, args: List[str]) -> None:
        try: 
            ctx = cli.make_context(cli.name, args)
            if args:
                cmd_name, cmd, cmd_args = cli.resolve_command(ctx, args)
                cmd.make_context(cmd_name, cmd_args, parent=ctx)
        except (NoSuchOption, UsageError) as e:
            raise DBTUsageException(e.message) 
        
        ctx.obj = {}
        # yikes?
        self.__dict__.update(ctx.__dict__)
        # TODO: consider initializing Flags, ctx.obj here.

    # @classmethod
    # def from_args(cls, args: List[str]) -> "DBTContext":
    #     try: 
    #         ctx = cli.make_context(cli.name, args)
    #         if args:
    #             cmd_name, cmd, cmd_args = cli.resolve_command(ctx, args)
    #             cmd.make_context(cmd_name, cmd_args, parent=ctx)
    #     except (NoSuchOption, UsageError) as e:
    #         raise DBTUsageException(e.message) 
        
    #     ctx.obj = {}
    #     # yikes
    #     ctx.__class__ = cls
    #     return ctx

    def set_project(self, project: Project):
        if not isinstance(project, Project):
            raise ValueError(f"{project} is a {type(project)}, expected a Project object.")
        
        self.obj["project"] = project