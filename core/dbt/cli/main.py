import click
from dbt.cli import params as p
import sys

# This is temporary for RAT-ing
import inspect
from pprint import pformat as pf


# dbt
@click.group(
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@p.version
@p.cache_selected_only
@p.debug
@p.fail_fast
@p.log_format
@p.partial_parse
@p.print
@p.printer_width
@p.quiet
@p.send_anonymous_usage_stats
@p.static_parser
@p.use_colors
@p.use_experimental_parser
@p.version_check
@p.warn_error
@p.write_json
@p.event_buffer_size
@p.record_timing
def cli(ctx, **kwargs):
    """An ELT tool for managing your SQL transformations and data models.
    For more documentation on these commands, visit: docs.getdbt.com
    """
    if kwargs.get("version", False):
        click.echo(f"`version` called\n ctx.params: {pf(ctx.params)}")
        sys.exit()
    else:
        del ctx.params["version"]


# dbt build
@cli.command("build")
@click.pass_context
def build(ctx, **kwargs):
    """Run all Seeds, Models, Snapshots, and tests in DAG order"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt clean
@cli.command("clean")
@click.pass_context
@p.project_dir
@p.profiles_dir
@p.profile
@p.target
@p.vars
def clean(ctx, **kwargs):
    """Delete all folders in the clean-targets list (usually the dbt_packages and target directories.)"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt docs
@cli.group()
@click.pass_context
def docs(ctx, **kwargs):
    """Generate or serve the documentation website for your project"""


# dbt docs generate
@docs.command("generate")
@click.pass_context
@p.version_check
@p.project_dir
@p.profiles_dir
@p.profile
@p.target
@p.vars
@p.compile_docs
@p.defer
@p.threads
@p.target_path
@p.log_path
@p.models
@p.exclude
@p.selector
@p.state
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt docs serve
@docs.command("serve")
@click.pass_context
@p.project_dir
@p.profiles_dir
@p.profile
@p.target
@p.vars
@p.port
@p.browser
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt compile
@cli.command("compile")
@click.pass_context
@p.version_check
@p.project_dir
@p.profiles_dir
@p.profile
@p.target
@p.vars
@p.parse_only
@p.threads
@p.target_path
@p.log_path
@p.models
@p.exclude
@p.selector
@p.state
@p.defer
@p.full_refresh
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files. Compiled SQL files are written to the target/ directory."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt debug
@cli.command("debug")
@click.pass_context
@p.version_check
@p.project_dir
@p.profiles_dir
@p.profile
@p.target
@p.vars
@p.config_dir
def debug(ctx, **kwargs):
    """Show some helpful information about dbt for debugging. Not to be confused with the --debug option which increases verbosity."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


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
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt init
@cli.command("init")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.skip_profile_setup
def init(ctx, **kwargs):
    """Initialize a new DBT project."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt list
# dbt TODO: Figure out aliasing for ls (or just c/p?)
@cli.command("list")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.output
@p.ouptut_keys
@p.resource_type
@p.models
@p.indirect_selection
@p.exclude
@p.selector
@p.state
def list(ctx, **kwargs):
    """List the resources in your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt parse
@cli.command("parse")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.write_manifest
@p.compile_parse
@p.threads
@p.target_path
@p.log_path
@p.version_check
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt run
@cli.command("run")
@click.pass_context
@p.fail_fast
@p.version_check
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.log_path
@p.target_path
@p.threads
@p.models
@p.exclude
@p.selector
@p.state
@p.defer
@p.full_refresh
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt run operation
@cli.command("run-operation")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.args
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt seed
@cli.command("seed")
@click.pass_context
@p.version_check
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.full_refresh
@p.log_path
@p.target_path
@p.threads
@p.models
@p.exclude
@p.selector
@p.state
@p.show
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse."""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt snapshot
@cli.command("snapshot")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.threads
@p.models
@p.exclude
@p.selector
@p.state
@p.defer
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# dbt source
@cli.group()
@click.pass_context
def source(ctx, **kwargs):
    """Manage your project's sources"""


# dbt source freshness
@source.command("freshness")
@click.pass_context
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.threads
@p.models
@p.exclude
@p.selector
@p.state
@p.output_path  # TODO: Is this ok to re-use?  We have three different output params, how much can we consolidate?
def freshness(ctx, **kwargs):
    """Snapshots the current freshness of the project's sources"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.parent.params)}"
    )


# dbt test
@cli.command("test")
@click.pass_context
@p.fail_fast
@p.version_check
@p.store_failures
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.vars
@p.indirect_selection
@p.log_path
@p.target_path
@p.threads
@p.models
@p.exclude
@p.selector
@p.state
@p.defer
def test(ctx, **kwargs):
    """Runs tests on data in deployed models. Run this after `dbt run`"""
    click.echo(
        f"`{inspect.stack()[0][3]}` called\n kwargs: {kwargs}\n ctx: {pf(ctx.parent.params)}"
    )


# Support running as a module
if __name__ == "__main__":
    cli()
