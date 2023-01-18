import inspect  # This is temporary for RAT-ing
from copy import copy
from pprint import pformat as pf  # This is temporary for RAT-ing
from typing import List, Tuple, Optional

import click
from dbt.cli import requires, params as p
from dbt.config import RuntimeConfig
from dbt.config.project import Project
from dbt.config.profile import Profile
from dbt.contracts.graph.manifest import Manifest
from dbt.task.clean import CleanTask
from dbt.task.compile import CompileTask
from dbt.task.deps import DepsTask
from dbt.task.run import RunTask
from dbt.task.test import TestTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.list import ListTask


# CLI invocation
def cli_runner():
    # Run the cli
    cli()


class dbtUsageException(Exception):
    pass


# Programmatic invocation
class dbtRunner:
    def __init__(
        self, project: Project = None, profile: Profile = None, manifest: Manifest = None
    ):
        self.project = project
        self.profile = profile
        self.manifest = manifest

    def invoke(self, args: List[str]) -> Tuple[Optional[List], bool]:
        try:
            dbt_ctx = cli.make_context(cli.name, args)
            dbt_ctx.obj = {
                "project": self.project,
                "profile": self.profile,
                "manifest": self.manifest,
            }
            return cli.invoke(dbt_ctx)
        except (click.NoSuchOption, click.UsageError) as e:
            raise dbtUsageException(e.message)


# dbt
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@p.anonymous_usage_stats
@p.cache_selected_only
@p.debug
@p.enable_legacy_logger
@p.fail_fast
@p.log_cache_events
@p.log_format
@p.log_path
@p.macro_debugging
@p.partial_parse
@p.print
@p.printer_width
@p.quiet
@p.record_timing_info
@p.single_threaded
@p.static_parser
@p.use_colors
@p.use_experimental_parser
@p.version
@p.version_check
@p.warn_error
@p.write_json
def cli(ctx, **kwargs):
    """An ELT tool for managing your SQL transformations and data models.
    For more documentation on these commands, visit: docs.getdbt.com
    """
    # Version info
    if ctx.params["version"]:
        click.echo(f"`version` called\n ctx.params: {pf(ctx.params)}")
        return


# dbt build
@cli.command("build")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.full_refresh
@p.indirect_selection
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.show
@p.state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
def build(ctx, **kwargs):
    """Run all Seeds, Models, Snapshots, and tests in DAG order"""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt clean
@cli.command("clean")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@requires.preflight
@requires.profile
@requires.project
def clean(ctx, **kwargs):
    """Delete all folders in the clean-targets list (usually the dbt_packages and target directories.)"""
    task = CleanTask(ctx.obj["flags"], ctx.obj["project"])

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt docs
@cli.group()
@click.pass_context
def docs(ctx, **kwargs):
    """Generate or serve the documentation website for your project"""


# dbt docs generate
@docs.command("generate")
@click.pass_context
@p.compile_docs
@p.defer
@p.exclude
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project"""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt docs serve
@docs.command("serve")
@click.pass_context
@p.browser
@p.port
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@requires.preflight
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project"""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt compile
@cli.command("compile")
@click.pass_context
@p.defer
@p.exclude
@p.full_refresh
@p.models
@p.parse_only
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files. Compiled SQL files are written to the
    target/ directory."""
    config = RuntimeConfig.from_parts(ctx.obj["project"], ctx.obj["profile"], ctx.obj["flags"])
    task = CompileTask(ctx.obj["flags"], config)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt debug
@cli.command("debug")
@click.pass_context
@p.config_dir
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.version_check
@requires.preflight
def debug(ctx, **kwargs):
    """Show some helpful information about dbt for debugging. Not to be confused with the --debug option which increases verbosity."""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt deps
@cli.command("deps")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@requires.preflight
@requires.profile
@requires.project
def deps(ctx, **kwargs):
    """Pull the most recent version of the dependencies listed in packages.yml"""
    task = DepsTask(ctx.obj["flags"], ctx.obj["project"])

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt init
@cli.command("init")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.skip_profile_setup
@p.target
@p.vars
@requires.preflight
def init(ctx, **kwargs):
    """Initialize a new DBT project."""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt list
@cli.command("list")
@click.pass_context
@p.exclude
@p.indirect_selection
@p.models
@p.output
@p.output_keys
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.select
@p.selector
@p.state
@p.target
@p.vars
@requires.preflight
@requires.profile
@requires.project
def list(ctx, **kwargs):
    """List the resources in your project"""
    config = RuntimeConfig.from_parts(ctx.obj["project"], ctx.obj["profile"], ctx.obj["flags"])
    task = ListTask(ctx.obj["flags"], config)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


ls = copy(cli.commands["list"])
ls.hidden = True
cli.add_command(ls, "ls")


# dbt parse
@cli.command("parse")
@click.pass_context
@p.compile_parse
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@p.write_manifest
@requires.preflight
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance"""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt run
@cli.command("run")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.full_refresh
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
@requires.profile
@requires.project
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    config = RuntimeConfig.from_parts(ctx.obj["project"], ctx.obj["profile"], ctx.obj["flags"])
    task = RunTask(ctx.obj["flags"], config)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt run operation
@cli.command("run-operation")
@click.pass_context
@p.args
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@requires.preflight
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt seed
@cli.command("seed")
@click.pass_context
@p.exclude
@p.full_refresh
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.show
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse."""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt snapshot
@cli.command("snapshot")
@click.pass_context
@p.defer
@p.exclude
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.target
@p.threads
@p.vars
@requires.preflight
@requires.profile
@requires.project
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project"""
    config = RuntimeConfig.from_parts(ctx.obj["project"], ctx.obj["profile"], ctx.obj["flags"])
    task = SnapshotTask(ctx.obj["flags"], config)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt source
@cli.group()
@click.pass_context
def source(ctx, **kwargs):
    """Manage your project's sources"""


# dbt source freshness
@source.command("freshness")
@click.pass_context
@p.exclude
@p.models
@p.output_path  # TODO: Is this ok to re-use?  We have three different output params, how much can we consolidate?
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.target
@p.threads
@p.vars
@requires.preflight
def freshness(ctx, **kwargs):
    """Snapshots the current freshness of the project's sources"""
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {ctx.obj['flags']}")
    return None, True


# dbt test
@cli.command("test")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.indirect_selection
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@requires.preflight
def test(ctx, **kwargs):
    """Runs tests on data in deployed models. Run this after `dbt run`"""
    config = RuntimeConfig.from_parts(ctx.obj["project"], ctx.obj["profile"], ctx.obj["flags"])
    task = TestTask(ctx.obj["flags"], config)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# Support running as a module
if __name__ == "__main__":
    cli_runner()
