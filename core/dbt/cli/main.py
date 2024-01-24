import functools
from copy import copy
from dataclasses import dataclass
from typing import Callable, List, Optional, Union

import click
from click.exceptions import (
    Exit as ClickExit,
    BadOptionUsage,
    NoSuchOption,
    UsageError,
)

from dbt.cli import requires, params as p
from dbt.cli.exceptions import (
    DbtInternalException,
    DbtUsageException,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import (
    CatalogArtifact,
    RunExecutionResult,
)
from dbt.events.base_types import EventMsg
from dbt.task.build import BuildTask
from dbt.task.clean import CleanTask
from dbt.task.clone import CloneTask
from dbt.task.compile import CompileTask
from dbt.task.debug import DebugTask
from dbt.task.deps import DepsTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.init import InitTask
from dbt.task.list import ListTask
from dbt.task.retry import RetryTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.seed import SeedTask
from dbt.task.serve import ServeTask
from dbt.task.show import ShowTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask


@dataclass
class dbtRunnerResult:
    """Contains the result of an invocation of the dbtRunner"""

    success: bool

    exception: Optional[BaseException] = None
    result: Union[
        bool,  # debug
        CatalogArtifact,  # docs generate
        List[str],  # list/ls
        Manifest,  # parse
        None,  # clean, deps, init, source
        RunExecutionResult,  # build, compile, run, seed, snapshot, test, run-operation
    ] = None


# Programmatic invocation
class dbtRunner:
    def __init__(
        self,
        manifest: Optional[Manifest] = None,
        callbacks: Optional[List[Callable[[EventMsg], None]]] = None,
    ) -> None:
        self.manifest = manifest

        if callbacks is None:
            callbacks = []
        self.callbacks = callbacks

    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        try:
            dbt_ctx = cli.make_context(cli.name, args)
            dbt_ctx.obj = {
                "manifest": self.manifest,
                "callbacks": self.callbacks,
            }

            for key, value in kwargs.items():
                dbt_ctx.params[key] = value
                # Hack to set parameter source to custom string
                dbt_ctx.set_parameter_source(key, "kwargs")  # type: ignore

            result, success = cli.invoke(dbt_ctx)
            return dbtRunnerResult(
                result=result,
                success=success,
            )
        except requires.ResultExit as e:
            return dbtRunnerResult(
                result=e.result,
                success=False,
            )
        except requires.ExceptionExit as e:
            return dbtRunnerResult(
                exception=e.exception,
                success=False,
            )
        except (BadOptionUsage, NoSuchOption, UsageError) as e:
            return dbtRunnerResult(
                exception=DbtUsageException(e.message),
                success=False,
            )
        except ClickExit as e:
            if e.exit_code == 0:
                return dbtRunnerResult(success=True)
            return dbtRunnerResult(
                exception=DbtInternalException(f"unhandled exit code {e.exit_code}"),
                success=False,
            )
        except BaseException as e:
            return dbtRunnerResult(
                exception=e,
                success=False,
            )


# approach from https://github.com/pallets/click/issues/108#issuecomment-280489786
def global_flags(func):
    @p.cache_selected_only
    @p.debug
    @p.deprecated_print
    @p.enable_legacy_logger
    @p.fail_fast
    @p.log_cache_events
    @p.log_file_max_bytes
    @p.log_format_file
    @p.log_level
    @p.log_level_file
    @p.log_path
    @p.macro_debugging
    @p.partial_parse
    @p.partial_parse_file_path
    @p.partial_parse_file_diff
    @p.populate_cache
    @p.print
    @p.printer_width
    @p.quiet
    @p.record_timing_info
    @p.send_anonymous_usage_stats
    @p.single_threaded
    @p.static_parser
    @p.use_colors
    @p.use_colors_file
    @p.use_experimental_parser
    @p.version
    @p.version_check
    @p.write_json
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# dbt
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@global_flags
@p.warn_error
@p.warn_error_options
@p.log_format
@p.show_resource_report
def cli(ctx, **kwargs):
    """An ELT tool for managing your SQL transformations and data models.
    For more documentation on these commands, visit: docs.getdbt.com
    """


# dbt build
@cli.command("build")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.full_refresh
@p.include_saved_query
@p.indirect_selection
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.select
@p.selector
@p.show
@p.state
@p.defer_state
@p.deprecated_state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def build(ctx, **kwargs):
    """Run all seeds, models, snapshots, and tests in DAG order"""
    task = BuildTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt clean
@cli.command("clean")
@click.pass_context
@global_flags
@p.clean_project_files_only
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.unset_profile
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
@global_flags
def docs(ctx, **kwargs):
    """Generate or serve the documentation website for your project"""


# dbt docs generate
@docs.command("generate")
@click.pass_context
@global_flags
@p.compile_docs
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.empty_catalog
@p.static
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest(write=False)
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project"""
    task = GenerateTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt docs serve
@docs.command("serve")
@click.pass_context
@global_flags
@p.browser
@p.port
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project"""
    task = ServeTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt compile
@cli.command("compile")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.full_refresh
@p.show_output_format
@p.indirect_selection
@p.introspect
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.inline
@p.state
@p.defer_state
@p.deprecated_state
@p.compile_inject_ephemeral_ctes
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files. Compiled SQL files are written to the
    target/ directory."""
    task = CompileTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt show
@cli.command("show")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.full_refresh
@p.show_output_format
@p.show_limit
@p.indirect_selection
@p.introspect
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.inline
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def show(ctx, **kwargs):
    """Generates executable SQL for a named resource or inline query, runs that SQL, and returns a preview of the
    results. Does not materialize anything to the warehouse."""
    task = ShowTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt debug
@cli.command("debug")
@click.pass_context
@global_flags
@p.debug_connection
@p.config_dir
@p.profile
@p.profiles_dir_exists_false
@p.project_dir
@p.target
@p.vars
@requires.postflight
@requires.preflight
def debug(ctx, **kwargs):
    """Show information on the current dbt environment and check dependencies, then test the database connection. Not to be confused with the --debug option which increases verbosity."""

    task = DebugTask(
        ctx.obj["flags"],
        None,
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt deps
@cli.command("deps")
@click.pass_context
@global_flags
@p.profile
@p.profiles_dir_exists_false
@p.project_dir
@p.target
@p.vars
@p.source
@p.dry_run
@p.lock
@p.upgrade
@p.add_package
@requires.postflight
@requires.preflight
@requires.unset_profile
@requires.project
def deps(ctx, **kwargs):
    """Install dbt packages specified.
    In the following case, a new `package-lock.yml` will be generated and the packages are installed:
    - user updated the packages.yml
    - user specify the flag --update, which means for packages that are specified as a
      range, dbt-core will try to install the newer version
    Otherwise, deps will use `package-lock.yml` as source of truth to install packages.

    There is a way to add new packages by providing an `--add-package` flag to deps command
    which will allow user to specify a package they want to add in the format of packagename@version.
    """
    flags = ctx.obj["flags"]
    if flags.ADD_PACKAGE:
        if not flags.ADD_PACKAGE["version"] and flags.SOURCE != "local":
            raise BadOptionUsage(
                message=f"Version is required in --add-package when a package when source is {flags.SOURCE}",
                option_name="--add-package",
            )
    else:
        if flags.DRY_RUN:
            raise BadOptionUsage(
                message="Invalid flag `--dry-run` when not using `--add-package`.",
                option_name="--dry-run",
            )
    task = DepsTask(flags, ctx.obj["project"])
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt init
@cli.command("init")
@click.pass_context
@global_flags
# for backwards compatibility, accept 'project_name' as an optional positional argument
@click.argument("project_name", required=False)
@p.profile
@p.profiles_dir_exists_false
@p.project_dir
@p.skip_profile_setup
@p.target
@p.vars
@requires.postflight
@requires.preflight
def init(ctx, **kwargs):
    """Initialize a new dbt project."""
    task = InitTask(ctx.obj["flags"], None)

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt list
@cli.command("list")
@click.pass_context
@global_flags
@p.exclude
@p.indirect_selection
@p.models
@p.output
@p.output_keys
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.raw_select
@p.selector
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def list(ctx, **kwargs):
    """List the resources in your project"""
    task = ListTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# Alias "list" to "ls"
ls = copy(cli.commands["list"])
ls.hidden = True
cli.add_command(ls, "ls")


# dbt parse
@cli.command("parse")
@click.pass_context
@global_flags
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest(write_perf_info=True)
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance"""
    # manifest generation and writing happens in @requires.manifest
    return ctx.obj["manifest"], True


# dbt run
@cli.command("run")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.favor_state
@p.deprecated_favor_state
@p.exclude
@p.full_refresh
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    task = RunTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt retry
@cli.command("retry")
@click.pass_context
@global_flags
@p.project_dir
@p.profiles_dir
@p.vars
@p.profile
@p.target
@p.state
@p.threads
@p.full_refresh
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
def retry(ctx, **kwargs):
    """Retry the nodes that failed in the previous run."""
    # Retry will parse manifest inside the task after we consolidate the flags
    task = RetryTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt clone
@cli.command("clone")
@click.pass_context
@global_flags
@p.defer_state
@p.exclude
@p.full_refresh
@p.profile
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.select
@p.selector
@p.state  # required
@p.target
@p.target_path
@p.threads
@p.vars
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
@requires.postflight
def clone(ctx, **kwargs):
    """Create clones of selected nodes based on their location in the manifest provided to --state."""
    task = CloneTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt run operation
@cli.command("run-operation")
@click.pass_context
@global_flags
@click.argument("macro")
@p.args
@p.profile
@p.profiles_dir
@p.project_dir
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    task = RunOperationTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt seed
@cli.command("seed")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.show
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse."""
    task = SeedTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt snapshot
@cli.command("snapshot")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project"""
    task = SnapshotTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# dbt source
@cli.group()
@click.pass_context
@global_flags
def source(ctx, **kwargs):
    """Manage your project's sources"""


# dbt source freshness
@source.command("freshness")
@click.pass_context
@global_flags
@p.exclude
@p.output_path  # TODO: Is this ok to re-use?  We have three different output params, how much can we consolidate?
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.defer_state
@p.deprecated_state
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def freshness(ctx, **kwargs):
    """check the current freshness of the project's sources"""
    task = FreshnessTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# Alias "source freshness" to "snapshot-freshness"
snapshot_freshness = copy(cli.commands["source"].commands["freshness"])  # type: ignore
snapshot_freshness.hidden = True
cli.commands["source"].add_command(snapshot_freshness, "snapshot-freshness")  # type: ignore


# dbt test
@cli.command("test")
@click.pass_context
@global_flags
@p.defer
@p.deprecated_defer
@p.exclude
@p.favor_state
@p.deprecated_favor_state
@p.indirect_selection
@p.profile
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.state
@p.defer_state
@p.deprecated_state
@p.store_failures
@p.target
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def test(ctx, **kwargs):
    """Runs tests on data in deployed models. Run this after `dbt run`"""
    task = TestTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )

    results = task.run()
    success = task.interpret_results(results)
    return results, success


# Support running as a module
if __name__ == "__main__":
    cli()
