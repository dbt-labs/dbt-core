import inspect  # This is temporary for RAT-ing
from copy import copy
from pprint import pformat as pf  # This is temporary for RAT-ing

import click
from dbt.adapters.factory import adapter_management
from dbt.cli import params as p
from dbt.cli.flags import Flags
from dbt.profiler import profiler


def cli_runner():
    # Alias "list" to "ls"
    ls = copy(cli.commands["list"])
    ls.hidden = True
    cli.add_command(ls, "ls")

    # Run the cli
    cli()


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
@p.macro_debugging
@p.partial_parse
@p.print
@p.printer_width
@p.quiet
@p.record_timing_info
@p.static_parser
@p.use_colors
@p.use_experimental_parser
@p.version
@p.version_check
@p.warn_error
@p.warn_error_options
@p.write_json
def cli(ctx, **kwargs):
    """An ELT tool for managing your SQL transformations and data models.
    For more documentation on these commands, visit: docs.getdbt.com
    """
    incomplete_flags = Flags()

    # Profiling
    if incomplete_flags.RECORD_TIMING_INFO:
        ctx.with_resource(profiler(enable=True, outfile=incomplete_flags.RECORD_TIMING_INFO))

    # Adapter management
    ctx.with_resource(adapter_management())

    # Version info
    if incomplete_flags.VERSION:
        click.echo(f"`version` called\n ctx.params: {pf(ctx.params)}")
        return
    else:
        del ctx.params["version"]


# dbt build
@cli.command("build")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.full_refresh
@p.indirect_selection
@p.log_path
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.show
@p.state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def build(ctx, **kwargs):
    """Run all Seeds, Models, Snapshots, and tests in DAG order"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt clean
@cli.command("clean")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
def clean(ctx, **kwargs):
    """Delete all folders in the clean-targets list (usually the dbt_packages and target directories.)"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


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
@p.log_path
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


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
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt compile
@cli.command("compile")
@click.pass_context
@p.defer
@p.exclude
@p.full_refresh
@p.log_path
@p.models
@p.parse_only
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files. Compiled SQL files are written to the target/ directory."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


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
def debug(ctx, **kwargs):
    """Show some helpful information about dbt for debugging. Not to be confused with the --debug option which increases verbosity."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt deps
@cli.command("deps")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
def deps(ctx, **kwargs):
    """Pull the most recent version of the dependencies listed in packages.yml"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt init
@cli.command("init")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.skip_profile_setup
@p.target
@p.vars
def init(ctx, **kwargs):
    """Initialize a new DBT project."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


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
@p.selector
@p.state
@p.target
@p.vars
def list(ctx, **kwargs):
    """List the resources in your project"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt parse
@cli.command("parse")
@click.pass_context
@p.compile_parse
@p.log_path
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
@p.write_manifest
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt run
@cli.command("run")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.full_refresh
@p.log_path
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt run operation
@cli.command("run-operation")
@click.pass_context
@p.args
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt seed
@cli.command("seed")
@click.pass_context
@p.exclude
@p.full_refresh
@p.log_path
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.show
@p.state
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse."""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt snapshot
@cli.command("snapshot")
@click.pass_context
@p.defer
@p.exclude
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.state
@p.target
@p.threads
@p.vars
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


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
@p.selector
@p.state
@p.target
@p.threads
@p.vars
def freshness(ctx, **kwargs):
    """Snapshots the current freshness of the project's sources"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# dbt test
@cli.command("test")
@click.pass_context
@p.defer
@p.exclude
@p.fail_fast
@p.indirect_selection
@p.log_path
@p.models
@p.profile
@p.profiles_dir
@p.project_dir
@p.selector
@p.state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@p.version_check
def test(ctx, **kwargs):
    """Runs tests on data in deployed models. Run this after `dbt run`"""
    flags = Flags()
    click.echo(f"`{inspect.stack()[0][3]}` called\n flags: {flags}")


# Support running as a module
if __name__ == "__main__":
    cli_runner()
