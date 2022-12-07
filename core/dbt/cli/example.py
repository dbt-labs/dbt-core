import click
import os
import sys

from dbt.cli import dbt_cli
from dbt.cli.context import make_context
from dbt.adapters.factory import adapter_management
from dbt.profiler import profiler
from dbt.config.runtime import load_project

# python core/dbt/cli/example.py deps --project-dir <project-dir-path>
# python core/dbt/cli/example.py run --project-dir <project-dir-path>
if __name__ == "__main__":

    # currently this would not construct params properly
    # Use cli group to configure context + call arbitrary command
    # ctx = make_context(cli_args)
    # if ctx:
    #     dbt.invoke(ctx)

    # Bypass cli group context configuration entirely and invoke deps directly
    # Note: This only really works because of the prior global initializations (logging, tracking) from dbt.invoke(ctx)
    input_args = sys.argv[1:]
    # we are not supporting --version, --help in this example for now.
    command = input_args[0]
    cli_args = input_args[1:]
    click.echo(f"\n`dbt {command}` called")
    ctx = make_context(cli_args, dbt_cli.commands[command])
    assert ctx is not None

    ctx.with_resource(adapter_management())
    ctx.with_resource(profiler(enable=True, outfile="output.profile"))
    project_dir = os.path.expanduser(ctx.params.get("project_dir"))  # type: ignore
    ctx.obj["project"] = load_project(project_dir, True, None, None)  # type: ignore
    dbt_cli.commands[command].invoke(ctx)
