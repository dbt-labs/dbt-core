from typing import List
from dbt.logger import log_cache_events, log_manager

import argparse
import os.path
import sys
import traceback
from contextlib import contextmanager
from pathlib import Path

import dbt.version
from dbt.events.functions import fire_event, setup_event_logger
from dbt.events.types import (
    MainEncounteredError,
    MainKeyboardInterrupt,
    MainReportVersion,
    MainReportArgs,
    MainTrackingUserState,
    MainStackTrace,
)
import dbt.flags as flags
import dbt.task.build as build_task
import dbt.task.clean as clean_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.deps as deps_task
import dbt.task.freshness as freshness_task
import dbt.task.generate as generate_task
import dbt.task.init as init_task
import dbt.task.list as list_task
import dbt.task.parse as parse_task
import dbt.task.run as run_task
import dbt.task.run_operation as run_operation_task
import dbt.task.seed as seed_task
import dbt.task.serve as serve_task
import dbt.task.snapshot as snapshot_task
import dbt.task.test as test_task
from dbt.profiler import profiler
from dbt.adapters.factory import reset_adapters, cleanup_connections

import dbt.tracking

from dbt.utils import ExitCodes, args_to_dict
from dbt.config.profile import DEFAULT_PROFILES_DIR, read_user_config
from dbt.exceptions import InternalException, NotImplementedException, FailedToConnectException


class DBTVersion(argparse.Action):
    """This is very similar to the built-in argparse._Version action,
    except it just calls dbt.version.get_version_information().
    """

    def __init__(
        self,
        option_strings,
        version=None,
        dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS,
        help="show program's version number and exit",
    ):
        super().__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        formatter = argparse.RawTextHelpFormatter(prog=parser.prog)
        formatter.add_text(dbt.version.get_version_information())
        parser.exit(message=formatter.format_help())


class DBTArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register("action", "dbtversion", DBTVersion)

    def add_optional_argument_inverse(
        self,
        name,
        *,
        enable_help=None,
        disable_help=None,
        dest=None,
        no_name=None,
        default=None,
    ):
        mutex_group = self.add_mutually_exclusive_group()
        if not name.startswith("--"):
            raise InternalException(
                'cannot handle optional argument without "--" prefix: ' f'got "{name}"'
            )
        if dest is None:
            dest_name = name[2:].replace("-", "_")
        else:
            dest_name = dest

        if no_name is None:
            no_name = f"--no-{name[2:]}"

        mutex_group.add_argument(
            name,
            action="store_const",
            const=True,
            dest=dest_name,
            default=default,
            help=enable_help,
        )

        mutex_group.add_argument(
            f"--no-{name[2:]}",
            action="store_const",
            const=False,
            dest=dest_name,
            default=default,
            help=disable_help,
        )

        return mutex_group


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    with log_manager.applicationbound():
        try:
            results, succeeded = handle_and_check(args)
            if succeeded:
                exit_code = ExitCodes.Success.value
            else:
                exit_code = ExitCodes.ModelError.value

        except KeyboardInterrupt:
            # if the logger isn't configured yet, it will use the default logger
            fire_event(MainKeyboardInterrupt())
            exit_code = ExitCodes.UnhandledError.value

        # This can be thrown by eg. argparse
        except SystemExit as e:
            exit_code = e.code

        except BaseException as e:
            fire_event(MainEncounteredError(e=str(e)))
            fire_event(MainStackTrace(stack_trace=traceback.format_exc()))
            exit_code = ExitCodes.UnhandledError.value

    sys.exit(exit_code)


# here for backwards compatibility
def handle(args):
    res, success = handle_and_check(args)
    return res


@contextmanager
def adapter_management():
    reset_adapters()
    try:
        yield
    finally:
        cleanup_connections()


def handle_and_check(args):
    with log_manager.applicationbound():
        parsed = parse_args(args)

        # Set flags from args, user config, and env vars
        user_config = read_user_config(flags.PROFILES_DIR)  # This is read again later
        flags.set_from_args(parsed, user_config)
        dbt.tracking.initialize_from_flags()
        # Set log_format from flags
        parsed.cls.set_log_format()

        # we've parsed the args and set the flags - we can now decide if we're debug or not
        if flags.DEBUG:
            log_manager.set_debug()

        profiler_enabled = False

        if parsed.record_timing_info:
            profiler_enabled = True

        with profiler(enable=profiler_enabled, outfile=parsed.record_timing_info):

            with adapter_management():

                task, res = run_from_args(parsed)
                success = task.interpret_results(res)

            return res, success


@contextmanager
def track_run(task):
    dbt.tracking.track_invocation_start(config=task.config, args=task.args)
    try:
        yield
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="ok")
    except (NotImplementedException, FailedToConnectException) as e:
        fire_event(MainEncounteredError(e=str(e)))
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="error")
    except Exception:
        dbt.tracking.track_invocation_end(config=task.config, args=task.args, result_type="error")
        raise
    finally:
        dbt.tracking.flush()


def run_from_args(parsed):
    log_cache_events(getattr(parsed, "log_cache_events", False))

    # this will convert DbtConfigErrors into RuntimeExceptions
    # task could be any one of the task objects
    task = parsed.cls.from_args(args=parsed)

    # Set up logging
    log_path = None
    if task.config is not None:
        log_path = getattr(task.config, "log_path", None)
    log_manager.set_path(log_path)
    # if 'list' task: set stdout to WARN instead of INFO
    level_override = parsed.cls.pre_init_hook(parsed)
    setup_event_logger(log_path or "logs", level_override)

    fire_event(MainReportVersion(v=str(dbt.version.installed)))
    fire_event(MainReportArgs(args=args_to_dict(parsed)))

    if dbt.tracking.active_user is not None:  # mypy appeasement, always true
        fire_event(MainTrackingUserState(user_state=dbt.tracking.active_user.state()))

    results = None

    with track_run(task):
        results = task.run()
    return task, results


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        "--project-dir",
        default=None,
        type=str,
        help="""
        Which directory to look in for the dbt_project.yml file.
        Default is the current working directory and its parents.
        """,
    )

    base_subparser.add_argument(
        "--profiles-dir",
        default=None,
        dest="sub_profiles_dir",  # Main cli arg precedes subcommand
        type=str,
        help="""
        Which directory to look in for the profiles.yml file. Default = {}
        """.format(
            DEFAULT_PROFILES_DIR
        ),
    )

    base_subparser.add_argument(
        "--profile",
        required=False,
        type=str,
        help="""
        Which profile to load. Overrides setting in dbt_project.yml.
        """,
    )

    base_subparser.add_argument(
        "-t",
        "--target",
        default=None,
        type=str,
        help="""
        Which target to load for the given profile
        """,
    )

    base_subparser.add_argument(
        "--vars",
        type=str,
        default="{}",
        help="""
        Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}'
        """,
    )

    # if set, log all cache events. This is extremely verbose!
    base_subparser.add_argument(
        "--log-cache-events",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser


def _build_docs_subparser(subparsers, base_subparser):
    docs_sub = subparsers.add_parser(
        "docs",
        help="""
        Generate or serve the documentation website for your project.
        """,
    )
    return docs_sub


def _build_source_subparser(subparsers, base_subparser):
    source_sub = subparsers.add_parser(
        "source",
        help="""
        Manage your project's sources
        """,
    )
    return source_sub


def _build_init_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "init",
        parents=[base_subparser],
        help="""
        Initialize a new DBT project.
        """,
    )
    sub.add_argument(
        "project_name",
        nargs="?",
        help="""
        Name of the new DBT project.
        """,
    )
    sub.add_argument(
        "-s",
        "--skip-profile-setup",
        dest="skip_profile_setup",
        action="store_true",
        help="""
        Skip interative profile setup.
        """,
    )
    sub.set_defaults(cls=init_task.InitTask, which="init", rpc_method=None)
    return sub


def _build_build_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "build",
        parents=[base_subparser],
        help="""
        Run all Seeds, Models, Snapshots, and tests in DAG order
        """,
    )
    sub.set_defaults(cls=build_task.BuildTask, which="build", rpc_method="build")
    sub.add_argument(
        "-x",
        "--fail-fast",
        dest="sub_fail_fast",
        action="store_true",
        help="""
        Stop execution upon a first failure.
        """,
    )
    sub.add_argument(
        "--store-failures",
        action="store_true",
        help="""
        Store test results (failing rows) in the database
        """,
    )
    sub.add_argument(
        "--indirect-selection",
        choices=["eager", "cautious"],
        default="eager",
        dest="indirect_selection",
        help="""
            Select all tests that are adjacent to selected resources,
            even if they those resources have been explicitly selected.
        """,
    )

    resource_values: List[str] = [str(s) for s in build_task.BuildTask.ALL_RESOURCE_VALUES] + [
        "all"
    ]
    sub.add_argument(
        "--resource-type",
        choices=resource_values,
        action="append",
        default=[],
        dest="resource_types",
    )
    # explicity don't support --models
    sub.add_argument(
        "-s",
        "--select",
        dest="select",
        nargs="+",
        help="""
            Specify the nodes to include.
        """,
    )
    _add_common_selector_arguments(sub)
    return sub


def _build_clean_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "clean",
        parents=[base_subparser],
        help="""
        Delete all folders in the clean-targets list
        (usually the dbt_packages and target directories.)
        """,
    )
    sub.set_defaults(cls=clean_task.CleanTask, which="clean", rpc_method=None)
    return sub


def _build_debug_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "debug",
        parents=[base_subparser],
        help="""
        Show some helpful information about dbt for debugging.

        Not to be confused with the --debug option which increases verbosity.
        """,
    )
    sub.add_argument(
        "--config-dir",
        action="store_true",
        help="""
        If specified, DBT will show path information for this project
        """,
    )
    _add_version_check(sub)
    sub.set_defaults(cls=debug_task.DebugTask, which="debug", rpc_method=None)
    return sub


def _build_deps_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "deps",
        parents=[base_subparser],
        help="""
        Pull the most recent version of the dependencies listed in packages.yml
        """,
    )
    sub.set_defaults(cls=deps_task.DepsTask, which="deps", rpc_method="deps")
    return sub


def _build_snapshot_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "snapshot",
        parents=[base_subparser],
        help="""
        Execute snapshots defined in your project
        """,
    )
    sub.add_argument(
        "--threads",
        type=int,
        required=False,
        help="""
        Specify number of threads to use while snapshotting tables.
        Overrides settings in profiles.yml.
        """,
    )
    sub.set_defaults(cls=snapshot_task.SnapshotTask, which="snapshot", rpc_method="snapshot")
    return sub


def _add_defer_argument(*subparsers):
    for sub in subparsers:
        sub.add_optional_argument_inverse(
            "--defer",
            enable_help="""
            If set, defer to the state variable for resolving unselected nodes.
            """,
            disable_help="""
            If set, do not defer to the state variable for resolving unselected
            nodes.
            """,
            default=flags.DEFER_MODE,
        )


def _build_run_subparser(subparsers, base_subparser):
    run_sub = subparsers.add_parser(
        "run",
        parents=[base_subparser],
        help="""
        Compile SQL and execute against the current target database.
        """,
    )
    run_sub.add_argument(
        "-x",
        "--fail-fast",
        dest="sub_fail_fast",
        action="store_true",
        help="""
        Stop execution upon a first failure.
        """,
    )

    run_sub.set_defaults(cls=run_task.RunTask, which="run", rpc_method="run")
    return run_sub


def _build_compile_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "compile",
        parents=[base_subparser],
        help="""
        Generates executable SQL from source, model, test, and analysis files.
        Compiled SQL files are written to the target/ directory.
        """,
    )
    sub.set_defaults(cls=compile_task.CompileTask, which="compile", rpc_method="compile")
    sub.add_argument("--parse-only", action="store_true")
    return sub


def _build_parse_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "parse",
        parents=[base_subparser],
        help="""
        Parsed the project and provides information on performance
        """,
    )
    sub.set_defaults(cls=parse_task.ParseTask, which="parse", rpc_method="parse")
    sub.add_argument("--write-manifest", action="store_true")
    sub.add_argument("--compile", action="store_true")
    return sub


def _build_docs_generate_subparser(subparsers, base_subparser):
    # it might look like docs_sub is the correct parents entry, but that
    # will cause weird errors about 'conflicting option strings'.
    generate_sub = subparsers.add_parser("generate", parents=[base_subparser])
    generate_sub.set_defaults(
        cls=generate_task.GenerateTask, which="generate", rpc_method="docs.generate"
    )
    generate_sub.add_argument(
        "--no-compile",
        action="store_false",
        dest="compile",
        help="""
        Do not run "dbt compile" as part of docs generation
        """,
    )
    return generate_sub


def _add_common_selector_arguments(sub):
    sub.add_argument(
        "--exclude",
        required=False,
        nargs="+",
        help="""
            Specify the models to exclude.
        """,
    )
    sub.add_argument(
        "--selector",
        dest="selector_name",
        metavar="SELECTOR_NAME",
        help="""
        The selector name to use, as defined in selectors.yml
        """,
    )
    sub.add_argument(
        "--state",
        help="""
        If set, use the given directory as the source for json files to
        compare with this project.
        """,
        type=Path,
        default=flags.ARTIFACT_STATE_PATH,
    )


def _add_selection_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            "-m",
            "--models",
            dest="select",
            nargs="+",
            help="""
                Specify the nodes to include.
            """,
        )
        sub.add_argument(
            "-s",
            "--select",
            dest="select",
            nargs="+",
            help="""
                Specify the nodes to include.
            """,
        )
        _add_common_selector_arguments(sub)


def _add_table_mutability_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            "--full-refresh",
            action="store_true",
            help="""
            If specified, dbt will drop incremental models and
            fully-recalculate the incremental table from the model definition.
            """,
        )


def _add_version_check(sub):
    sub.add_argument(
        "--no-version-check",
        dest="sub_version_check",  # main cli arg precedes subcommands
        action="store_false",
        default=None,
        help="""
        If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
        """,
    )


def _add_common_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            "--threads",
            type=int,
            required=False,
            help="""
            Specify number of threads to use while executing models. Overrides
            settings in profiles.yml.
            """,
        )
        _add_version_check(sub)


def _build_seed_subparser(subparsers, base_subparser):
    seed_sub = subparsers.add_parser(
        "seed",
        parents=[base_subparser],
        help="""
        Load data from csv files into your data warehouse.
        """,
    )
    seed_sub.add_argument(
        "--full-refresh",
        action="store_true",
        help="""
        Drop existing seed tables and recreate them
        """,
    )
    seed_sub.add_argument(
        "--show",
        action="store_true",
        help="""
        Show a sample of the loaded data in the terminal
        """,
    )
    seed_sub.set_defaults(cls=seed_task.SeedTask, which="seed", rpc_method="seed")
    return seed_sub


def _build_docs_serve_subparser(subparsers, base_subparser):
    serve_sub = subparsers.add_parser("serve", parents=[base_subparser])
    serve_sub.add_argument(
        "--port",
        default=8080,
        type=int,
        help="""
        Specify the port number for the docs server.
        """,
    )
    serve_sub.add_argument(
        "--no-browser",
        dest="open_browser",
        action="store_false",
    )
    serve_sub.set_defaults(cls=serve_task.ServeTask, which="serve", rpc_method=None)
    return serve_sub


def _build_test_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "test",
        parents=[base_subparser],
        help="""
        Runs tests on data in deployed models. Run this after `dbt run`
        """,
    )
    sub.add_argument(
        "-x",
        "--fail-fast",
        dest="sub_fail_fast",
        action="store_true",
        help="""
        Stop execution upon a first test failure.
        """,
    )
    sub.add_argument(
        "--store-failures",
        action="store_true",
        help="""
        Store test results (failing rows) in the database
        """,
    )
    sub.add_argument(
        "--indirect-selection",
        choices=["eager", "cautious"],
        default="eager",
        dest="indirect_selection",
        help="""
            Select all tests that are adjacent to selected resources,
            even if they those resources have been explicitly selected.
        """,
    )

    sub.set_defaults(cls=test_task.TestTask, which="test", rpc_method="test")
    return sub


def _build_source_freshness_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "freshness",
        parents=[base_subparser],
        help="""
        Snapshots the current freshness of the project's sources
        """,
        aliases=["snapshot-freshness"],
    )
    sub.add_argument(
        "-o",
        "--output",
        required=False,
        help="""
        Specify the output path for the json report. By default, outputs to
        target/sources.json
        """,
    )
    sub.add_argument(
        "--threads",
        type=int,
        required=False,
        help="""
        Specify number of threads to use. Overrides settings in profiles.yml
        """,
    )
    sub.set_defaults(
        cls=freshness_task.FreshnessTask,
        which="source-freshness",
        rpc_method="source-freshness",
    )
    sub.add_argument(
        "-s",
        "--select",
        dest="select",
        nargs="+",
        help="""
            Specify the nodes to include.
        """,
    )
    _add_common_selector_arguments(sub)
    return sub


def _build_list_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "list",
        parents=[base_subparser],
        help="""
        List the resources in your project
        """,
        aliases=["ls"],
    )
    sub.set_defaults(cls=list_task.ListTask, which="list", rpc_method=None)
    resource_values: List[str] = [str(s) for s in list_task.ListTask.ALL_RESOURCE_VALUES] + [
        "default",
        "all",
    ]
    sub.add_argument(
        "--resource-type",
        choices=resource_values,
        action="append",
        default=[],
        dest="resource_types",
    )
    sub.add_argument("--output", choices=["json", "name", "path", "selector"], default="selector")
    sub.add_argument("--output-keys")

    sub.add_argument(
        "-m",
        "--models",
        dest="models",
        nargs="+",
        help="""
        Specify the models to select and set the resource-type to 'model'.
        Mutually exclusive with '--select' (or '-s') and '--resource-type'
        """,
        metavar="SELECTOR",
        required=False,
    )
    sub.add_argument(
        "-s",
        "--select",
        dest="select",
        nargs="+",
        help="""
            Specify the nodes to include.
        """,
        metavar="SELECTOR",
        required=False,
    )
    sub.add_argument(
        "--indirect-selection",
        choices=["eager", "cautious"],
        default="eager",
        dest="indirect_selection",
        help="""
            Select all tests that are adjacent to selected resources,
            even if they those resources have been explicitly selected.
        """,
    )
    _add_common_selector_arguments(sub)

    return sub


def _build_run_operation_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        "run-operation",
        parents=[base_subparser],
        help="""
        Run the named macro with any supplied arguments.
        """,
    )
    sub.add_argument(
        "macro",
        help="""
        Specify the macro to invoke. dbt will call this macro with the supplied
        arguments and then exit
        """,
    )
    sub.add_argument(
        "--args",
        type=str,
        default="{}",
        help="""
        Supply arguments to the macro. This dictionary will be mapped to the
        keyword arguments defined in the selected macro. This argument should
        be a YAML string, eg. '{my_variable: my_value}'
        """,
    )
    sub.set_defaults(
        cls=run_operation_task.RunOperationTask, which="run-operation", rpc_method="run-operation"
    )
    return sub


def parse_args(args, cls=DBTArgumentParser):
    p = cls(
        prog="dbt",
        description="""
        An ELT tool for managing your SQL transformations and data models.
        For more documentation on these commands, visit: docs.getdbt.com
        """,
        epilog="""
        Specify one of these sub-commands and you can find more help from
        there.
        """,
    )

    p.add_argument(
        "--version",
        action="dbtversion",
        help="""
        Show version information
        """,
    )

    p.add_argument(
        "-r",
        "--record-timing-info",
        default=None,
        type=str,
        help="""
        When this option is passed, dbt will output low-level timing stats to
        the specified file. Example: `--record-timing-info output.profile`
        """,
    )

    p.add_argument(
        "-d",
        "--debug",
        action="store_true",
        default=None,
        help="""
        Display debug logging during dbt execution. Useful for debugging and
        making bug reports.
        """,
    )

    p.add_argument(
        "--log-format",
        choices=["text", "json", "default"],
        default=None,
        help="""Specify the log format, overriding the command's default.""",
    )

    p.add_argument(
        "--no-write-json",
        action="store_false",
        default=None,
        dest="write_json",
        help="""
        If set, skip writing the manifest and run_results.json files to disk
        """,
    )
    colors_flag = p.add_mutually_exclusive_group()
    colors_flag.add_argument(
        "--use-colors",
        action="store_const",
        const=True,
        default=None,
        dest="use_colors",
        help="""
        Colorize the output DBT prints to the terminal. Output is colorized by
        default and may also be set in a profile or at the command line.
        Mutually exclusive with --no-use-colors
        """,
    )
    colors_flag.add_argument(
        "--no-use-colors",
        action="store_const",
        const=False,
        dest="use_colors",
        help="""
        Do not colorize the output DBT prints to the terminal. Output is
        colorized by default and may also be set in a profile or at the
        command line.
        Mutually exclusive with --use-colors
        """,
    )

    p.add_argument(
        "--printer-width",
        dest="printer_width",
        help="""
        Sets the width of terminal output
        """,
    )

    p.add_argument(
        "--warn-error",
        action="store_true",
        default=None,
        help="""
        If dbt would normally warn, instead raise an exception. Examples
        include --models that selects nothing, deprecations, configurations
        with no associated models, invalid test configurations, and missing
        sources/refs in tests.
        """,
    )

    p.add_argument(
        "--no-version-check",
        dest="version_check",
        action="store_false",
        default=None,
        help="""
        If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
        """,
    )

    p.add_optional_argument_inverse(
        "--partial-parse",
        enable_help="""
        Allow for partial parsing by looking for and writing to a pickle file
        in the target directory. This overrides the user configuration file.
        """,
        disable_help="""
        Disallow partial parsing. This overrides the user configuration file.
        """,
    )

    # if set, run dbt in single-threaded mode: thread count is ignored, and
    # calls go through `map` instead of the thread pool. This is useful for
    # getting performance information about aspects of dbt that normally run in
    # a thread, as the profiler ignores child threads. Users should really
    # never use this.
    p.add_argument(
        "--single-threaded",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    # if set, will use the latest features from the static parser instead of
    # the stable static parser.
    p.add_argument(
        "--use-experimental-parser",
        action="store_true",
        default=None,
        help="""
        Enables experimental parsing features.
        """,
    )

    # if set, will disable the use of the stable static parser and instead
    # always rely on jinja rendering.
    p.add_argument(
        "--no-static-parser",
        default=None,
        dest="static_parser",
        action="store_false",
        help="""
        Disables the static parser.
        """,
    )

    p.add_argument(
        "--profiles-dir",
        default=None,
        dest="profiles_dir",
        type=str,
        help="""
        Which directory to look in for the profiles.yml file. Default = {}
        """.format(
            DEFAULT_PROFILES_DIR
        ),
    )

    p.add_argument(
        "--no-anonymous-usage-stats",
        action="store_false",
        default=None,
        dest="send_anonymous_usage_stats",
        help="""
        Do not send anonymous usage stat to dbt Labs
        """,
    )

    p.add_argument(
        "-x",
        "--fail-fast",
        dest="fail_fast",
        action="store_true",
        default=None,
        help="""
        Stop execution upon a first failure.
        """,
    )

    p.add_argument(
        "--event-buffer-size",
        dest="event_buffer_size",
        help="""
        Sets the max number of events to buffer in EVENT_HISTORY
        """,
    )

    p.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=None,
        help="""
        Suppress all non-error logging to stdout. Does not affect
        {{ print() }} macro calls.
        """,
    )

    subs = p.add_subparsers(title="Available sub-commands")

    base_subparser = _build_base_subparser()

    # make the subcommands that have their own subcommands
    docs_sub = _build_docs_subparser(subs, base_subparser)
    docs_subs = docs_sub.add_subparsers(title="Available sub-commands")
    source_sub = _build_source_subparser(subs, base_subparser)
    source_subs = source_sub.add_subparsers(title="Available sub-commands")

    _build_init_subparser(subs, base_subparser)
    _build_clean_subparser(subs, base_subparser)
    _build_debug_subparser(subs, base_subparser)
    _build_deps_subparser(subs, base_subparser)
    _build_list_subparser(subs, base_subparser)

    build_sub = _build_build_subparser(subs, base_subparser)
    snapshot_sub = _build_snapshot_subparser(subs, base_subparser)
    run_sub = _build_run_subparser(subs, base_subparser)
    compile_sub = _build_compile_subparser(subs, base_subparser)
    parse_sub = _build_parse_subparser(subs, base_subparser)
    generate_sub = _build_docs_generate_subparser(docs_subs, base_subparser)
    test_sub = _build_test_subparser(subs, base_subparser)
    seed_sub = _build_seed_subparser(subs, base_subparser)
    # --threads, --no-version-check
    _add_common_arguments(
        run_sub, compile_sub, generate_sub, test_sub, seed_sub, parse_sub, build_sub
    )
    # --select, --exclude
    # list_sub sets up its own arguments.
    _add_selection_arguments(run_sub, compile_sub, generate_sub, test_sub, snapshot_sub, seed_sub)
    # --defer
    _add_defer_argument(run_sub, test_sub, build_sub, snapshot_sub)
    # --full-refresh
    _add_table_mutability_arguments(run_sub, compile_sub, build_sub)

    _build_docs_serve_subparser(docs_subs, base_subparser)
    _build_source_freshness_subparser(source_subs, base_subparser)
    _build_run_operation_subparser(subs, base_subparser)

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    # profiles_dir is set before subcommands and after, so normalize
    if hasattr(parsed, "sub_profiles_dir"):
        if parsed.sub_profiles_dir is not None:
            parsed.profiles_dir = parsed.sub_profiles_dir
        delattr(parsed, "sub_profiles_dir")
    if hasattr(parsed, "profiles_dir"):
        if parsed.profiles_dir is None:
            parsed.profiles_dir = flags.PROFILES_DIR
        else:
            parsed.profiles_dir = os.path.abspath(parsed.profiles_dir)
            # needs to be set before the other flags, because it's needed to
            # read the profile that contains them
            flags.PROFILES_DIR = parsed.profiles_dir

    # version_check is set before subcommands and after, so normalize
    if hasattr(parsed, "sub_version_check"):
        if parsed.sub_version_check is False:
            parsed.version_check = False
        delattr(parsed, "sub_version_check")

    # fail_fast is set before subcommands and after, so normalize
    if hasattr(parsed, "sub_fail_fast"):
        if parsed.sub_fail_fast is True:
            parsed.fail_fast = True
        delattr(parsed, "sub_fail_fast")

    if getattr(parsed, "project_dir", None) is not None:
        expanded_user = os.path.expanduser(parsed.project_dir)
        parsed.project_dir = os.path.abspath(expanded_user)

    if not hasattr(parsed, "which"):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed
