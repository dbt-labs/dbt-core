from dbt.logger import initialize_logger, GLOBAL_LOGGER as logger, \
    logger_initialized, log_cache_events

import argparse
import os.path
import sys
import traceback
from contextlib import contextmanager

import dbt.version
import dbt.flags as flags
import dbt.task.run as run_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.clean as clean_task
import dbt.task.deps as deps_task
import dbt.task.init as init_task
import dbt.task.seed as seed_task
import dbt.task.test as test_task
import dbt.task.archive as archive_task
import dbt.task.generate as generate_task
import dbt.task.serve as serve_task
import dbt.task.freshness as freshness_task
import dbt.task.run_operation as run_operation_task
from dbt.task.list import ListTask
from dbt.task.rpc_server import RPCServerTask
from dbt.adapters.factory import reset_adapters

import dbt.tracking
import dbt.ui.printer
import dbt.compat
import dbt.deprecations
import dbt.profiler

from dbt.utils import ExitCodes
from dbt.config import UserConfig, PROFILES_DIR
from dbt.exceptions import RuntimeException


PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""


class DBTVersion(argparse.Action):
    """This is very very similar to the builtin argparse._Version action,
    except it just calls dbt.version.get_version_information().
    """
    def __init__(self,
                 option_strings,
                 version=None,
                 dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS,
                 help="show program's version number and exit"):
        super(DBTVersion, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        formatter = parser._get_formatter()
        formatter.add_text(dbt.version.get_version_information())
        parser.exit(message=formatter.format_help())


class DBTArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(DBTArgumentParser, self).__init__(*args, **kwargs)
        self.register('action', 'dbtversion', DBTVersion)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    try:
        results, succeeded = handle_and_check(args)
        if succeeded:
            exit_code = ExitCodes.Success
        else:
            exit_code = ExitCodes.ModelError

    except KeyboardInterrupt:
        logger.info("ctrl-c")
        exit_code = ExitCodes.UnhandledError

    # This can be thrown by eg. argparse
    except SystemExit as e:
        exit_code = e.code

    except BaseException as e:
        logger.warn("Encountered an error:")
        logger.warn(str(e))

        if logger_initialized():
            logger.debug(traceback.format_exc())
        elif not isinstance(e, RuntimeException):
            # if it did not come from dbt proper and the logger is not
            # initialized (so there's no safe path to log to), log the stack
            # trace at error level.
            logger.error(traceback.format_exc())
        exit_code = ExitCodes.UnhandledError

    sys.exit(exit_code)


# here for backwards compatibility
def handle(args):
    res, success = handle_and_check(args)
    return res


def initialize_config_values(parsed):
    """Given the parsed args, initialize the dbt tracking code.

    It would be nice to re-use this profile later on instead of parsing it
    twice, but dbt's intialization is not structured in a way that makes that
    easy.
    """
    try:
        cfg = UserConfig.from_directory(parsed.profiles_dir)
    except RuntimeException:
        cfg = UserConfig.from_dict(None)

    cfg.set_values(parsed.profiles_dir)


def handle_and_check(args):
    parsed = parse_args(args)
    profiler_enabled = False

    if parsed.record_timing_info:
        profiler_enabled = True

    with dbt.profiler.profiler(
        enable=profiler_enabled,
        outfile=parsed.record_timing_info
    ):

        initialize_config_values(parsed)

        reset_adapters()

        task, res = run_from_args(parsed)
        success = task.interpret_results(res)

        return res, success


@contextmanager
def track_run(task):
    dbt.tracking.track_invocation_start(config=task.config, args=task.args)
    try:
        yield
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="ok"
        )
    except (dbt.exceptions.NotImplementedException,
            dbt.exceptions.FailedToConnectException) as e:
        logger.error('ERROR: {}'.format(e))
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="error"
        )
    except Exception:
        dbt.tracking.track_invocation_end(
            config=task.config, args=task.args, result_type="error"
        )
        raise
    finally:
        dbt.tracking.flush()


def run_from_args(parsed):
    log_cache_events(getattr(parsed, 'log_cache_events', False))
    update_flags(parsed)

    parsed.cls.pre_init_hook()
    logger.info("Running with dbt{}".format(dbt.version.installed))

    # this will convert DbtConfigErrors into RuntimeExceptions
    task = parsed.cls.from_args(args=parsed)
    logger.debug("running dbt with arguments %s", parsed)

    log_path = None
    if task.config is not None:
        log_path = getattr(task.config, 'log_path', None)
    initialize_logger(parsed.debug, log_path)
    logger.debug("Tracking: {}".format(dbt.tracking.active_user.state()))

    results = None

    with track_run(task):
        results = task.run()

    return task, results


def update_flags(parsed):
    flags.USE_CACHE = getattr(parsed, 'use_cache', True)

    arg_drop_existing = getattr(parsed, 'drop_existing', False)
    arg_full_refresh = getattr(parsed, 'full_refresh', False)
    flags.STRICT_MODE = getattr(parsed, 'strict', False)
    flags.WARN_ERROR = (
        flags.STRICT_MODE or
        getattr(parsed, 'warn_error', False)
    )

    if arg_drop_existing:
        dbt.deprecations.warn('drop-existing')
        flags.FULL_REFRESH = True
    elif arg_full_refresh:
        flags.FULL_REFRESH = True

    flags.TEST_NEW_PARSER = getattr(parsed, 'test_new_parser', False)


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--profiles-dir',
        default=PROFILES_DIR,
        type=str,
        help="""
        Which directory to look in for the profiles.yml file. Default = {}
        """.format(PROFILES_DIR)
    )

    base_subparser.add_argument(
        '--profile',
        required=False,
        type=str,
        help="""
        Which profile to load. Overrides setting in dbt_project.yml.
        """
    )

    base_subparser.add_argument(
        '--target',
        default=None,
        type=str,
        help='Which target to load for the given profile'
    )

    base_subparser.add_argument(
        '--vars',
        type=str,
        default='{}',
        help="""
            Supply variables to the project. This argument overrides
            variables defined in your dbt_project.yml file. This argument
            should be a YAML string, eg. '{my_variable: my_value}'"""
    )

    # if set, log all cache events. This is extremely verbose!
    base_subparser.add_argument(
        '--log-cache-events',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    base_subparser.add_argument(
        '--bypass-cache',
        action='store_false',
        dest='use_cache',
        help='If set, bypass the adapter-level cache of database state',
    )
    return base_subparser


def _build_docs_subparser(subparsers, base_subparser):
    docs_sub = subparsers.add_parser(
        'docs',
        parents=[base_subparser],
        help="Generate or serve the documentation "
        "website for your project.")
    return docs_sub


def _build_source_subparser(subparsers, base_subparser):
    source_sub = subparsers.add_parser(
        'source',
        parents=[base_subparser],
        help="Manage your project's sources")
    return source_sub


def _build_init_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'init',
        parents=[base_subparser],
        help="Initialize a new DBT project.")
    sub.add_argument('project_name', type=str, help='Name of the new project')
    sub.set_defaults(cls=init_task.InitTask, which='init')
    return sub


def _build_clean_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'clean',
        parents=[base_subparser],
        help="Delete all folders in the clean-targets list"
        "\n(usually the dbt_modules and target directories.)")
    sub.set_defaults(cls=clean_task.CleanTask, which='clean')
    return sub


def _build_debug_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'debug',
        parents=[base_subparser],
        help="Show some helpful information about dbt for debugging."
        "\nNot to be confused with the --debug option which increases "
        "verbosity.")
    sub.add_argument(
        '--config-dir',
        action='store_true',
        help="""
        If specified, DBT will show path information for this project
        """
    )
    sub.set_defaults(cls=debug_task.DebugTask, which='debug')
    return sub


def _build_deps_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'deps',
        parents=[base_subparser],
        help="Pull the most recent version of the dependencies "
        "listed in packages.yml")
    sub.set_defaults(cls=deps_task.DepsTask, which='deps')
    return sub


def _build_archive_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'archive',
        parents=[base_subparser],
        help="Record changes to a mutable table over time."
             "\nMust be configured in your dbt_project.yml.")
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help="""
        Specify number of threads to use while archiving tables. Overrides
        settings in profiles.yml.
        """
    )
    sub.set_defaults(cls=archive_task.ArchiveTask, which='archive')
    return sub


def _build_run_subparser(subparsers, base_subparser):
    run_sub = subparsers.add_parser(
        'run',
        parents=[base_subparser],
        help="Compile SQL and execute against the current "
        "target database.")
    run_sub.set_defaults(cls=run_task.RunTask, which='run')
    return run_sub


def _build_compile_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'compile',
        parents=[base_subparser],
        help="Generates executable SQL from source model, test, and"
        "analysis files. \nCompiled SQL files are written to the target/"
        "directory.")
    sub.set_defaults(cls=compile_task.CompileTask, which='compile')
    return sub


def _build_docs_generate_subparser(subparsers, base_subparser):
    # it might look like docs_sub is the correct parents entry, but that
    # will cause weird errors about 'conflicting option strings'.
    generate_sub = subparsers.add_parser('generate', parents=[base_subparser])
    generate_sub.set_defaults(cls=generate_task.GenerateTask,
                              which='generate')
    generate_sub.add_argument(
        '--no-compile',
        action='store_false',
        dest='compile',
        help='Do not run "dbt compile" as part of docs generation'
    )
    return generate_sub


def _add_selection_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            '-m',
            '--models',
            required=False,
            nargs='+',
            help="""
            Specify the models to include.
            """
        )
        sub.add_argument(
            '--exclude',
            required=False,
            nargs='+',
            help="""
            Specify the models to exclude.
            """
        )


def _add_table_mutability_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            '--full-refresh',
            action='store_true',
            help="""
            If specified, DBT will drop incremental models and
            fully-recalculate the incremental table from the model definition.
            """)


def _add_common_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            '--threads',
            type=int,
            required=False,
            help="""
            Specify number of threads to use while executing models. Overrides
            settings in profiles.yml.
            """
        )
        sub.add_argument(
            '--no-version-check',
            dest='version_check',
            action='store_false',
            help="""
            If set, skip ensuring dbt's version matches the one specified in
            the dbt_project.yml file ('require-dbt-version')
            """)


def _build_seed_subparser(subparsers, base_subparser):
    seed_sub = subparsers.add_parser(
        'seed',
        parents=[base_subparser],
        help="Load data from csv files into your data warehouse.")
    seed_sub.add_argument(
        '--drop-existing',
        action='store_true',
        help='(DEPRECATED) Use --full-refresh instead.'
    )
    seed_sub.add_argument(
        '--full-refresh',
        action='store_true',
        help='Drop existing seed tables and recreate them'
    )
    seed_sub.add_argument(
        '--show',
        action='store_true',
        help='Show a sample of the loaded data in the terminal'
    )
    seed_sub.set_defaults(cls=seed_task.SeedTask, which='seed')
    return seed_sub


def _build_docs_serve_subparser(subparsers, base_subparser):
    serve_sub = subparsers.add_parser('serve', parents=[base_subparser])
    serve_sub.add_argument(
        '--port',
        default=8080,
        type=int,
        help='Specify the port number for the docs server.'
    )
    serve_sub.set_defaults(cls=serve_task.ServeTask, which='serve')
    return serve_sub


def _build_test_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'test',
        parents=[base_subparser],
        help="Runs tests on data in deployed models."
        "Run this after `dbt run`")
    sub.add_argument(
        '--data',
        action='store_true',
        help='Run data tests defined in "tests" directory.'
    )
    sub.add_argument(
        '--schema',
        action='store_true',
        help='Run constraint validations from schema.yml files'
    )

    sub.set_defaults(cls=test_task.TestTask, which='test')
    return sub


def _build_source_snapshot_freshness_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'snapshot-freshness',
        parents=[base_subparser],
        help="Snapshots the current freshness of the project's sources",
    )
    sub.add_argument(
        '-s',
        '--select',
        required=False,
        nargs='+',
        help="""
        Specify the sources to snapshot freshness
        """,
        dest='selected'
    )
    sub.add_argument(
        '-o',
        '--output',
        required=False,
        help="""
        Specify the output path for the json report. By default, outputs to
        target/sources.json
        """
    )
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help="""
        Specify number of threads to use. Overrides settings in profiles.yml
        """
    )
    sub.set_defaults(cls=freshness_task.FreshnessTask,
                     which='snapshot-freshness')
    return sub


def _build_rpc_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'rpc',
        parents=[base_subparser],
        help='Start a json-rpc server',
    )
    sub.add_argument(
        '--host',
        default='0.0.0.0',
        help='Specify the host to listen on for the rpc server.'
    )
    sub.add_argument(
        '--port',
        default=8580,
        type=int,
        help='Specify the port number for the rpc server.'
    )
    sub.set_defaults(cls=RPCServerTask, which='rpc')
    # the rpc task does a 'compile', so we need these attributes to exist, but
    # we don't want users to be allowed to set them.
    sub.set_defaults(models=None, exclude=None)
    return sub


def _build_list_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'list',
        parents=[base_subparser],
        help='list models'
    )
    sub.set_defaults(cls=ListTask, which='list')
    resource_values = list(ListTask.ALL_RESOURCE_VALUES) + ['default', 'all']
    sub.add_argument('--resource-type',
                     choices=resource_values,
                     action='append',
                     default=[],
                     dest='resource_types')
    sub.add_argument('--output',
                     choices=['json', 'name', 'path', 'selector'],
                     default='selector')
    sub.add_argument(
        '-s',
        '--select',
        required=False,
        nargs='+',
        metavar='SELECTOR',
        help="Specify the nodes to select.",
    )
    sub.add_argument(
        '-m',
        '--models',
        required=False,
        nargs='+',
        metavar='SELECTOR',
        help="Specify the models to select and set the resource-type to "
              "'model'. Mutually exclusive with '--select' (or '-s') and "
              "'--resource-type'",
    )
    sub.add_argument(
        '--exclude',
        required=False,
        nargs='+',
        metavar='SELECTOR',
        help="Specify the models to exclude."
    )
    # in python 3.x you can use the 'aliases' kwarg, but in python 2.7 you get
    # to do this
    subparsers._name_parser_map['ls'] = sub
    return sub


def parse_args(args):
    p = DBTArgumentParser(
        prog='dbt: data build tool',
        formatter_class=argparse.RawTextHelpFormatter,
        description="An ELT tool for managing your SQL "
        "transformations and data models."
        "\nFor more documentation on these commands, visit: "
        "docs.getdbt.com",
        epilog="Specify one of these sub-commands and you can "
        "find more help from there.")

    p.add_argument(
        '--version',
        action='dbtversion',
        help="Show version information")

    p.add_argument(
        '-r',
        '--record-timing-info',
        default=None,
        type=str,
        help="""
        When this option is passed, dbt will output low-level timing
        stats to the specified file. Example:
        `--record-timing-info output.profile`
        """
    )

    p.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='''Display debug logging during dbt execution. Useful for
        debugging and making bug reports.''')

    p.add_argument(
        '-S',
        '--strict',
        action='store_true',
        help='''Run schema validations at runtime. This will surface
        bugs in dbt, but may incur a performance penalty.''')

    p.add_argument(
        '--warn-error',
        action='store_true',
        help='''If dbt would normally warn, instead raise an exception.
        Examples include --models that selects nothing, deprecations,
        configurations with no associated models, invalid test configurations,
        and missing sources/refs in tests''')

    # if set, run dbt in single-threaded mode: thread count is ignored, and
    # calls go through `map` instead of the thread pool. This is useful for
    # getting performance information about aspects of dbt that normally run in
    # a thread, as the profiler ignores child threads. Users should really
    # never use this.
    p.add_argument(
        '--single-threaded',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    # if set, extract all models and blocks with the jinja block extractor, and
    # verify that we don't fail anywhere the actual jinja parser passes. The
    # reverse (passing files that ends up failing jinja) is fine.
    p.add_argument(
        '--test-new-parser',
        action='store_true',
        help=argparse.SUPPRESS
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

    archive_sub = _build_archive_subparser(subs, base_subparser)
    rpc_sub = _build_rpc_subparser(subs, base_subparser)
    run_sub = _build_run_subparser(subs, base_subparser)
    compile_sub = _build_compile_subparser(subs, base_subparser)
    generate_sub = _build_docs_generate_subparser(docs_subs, base_subparser)
    test_sub = _build_test_subparser(subs, base_subparser)
    # --threads, --no-version-check
    _add_common_arguments(run_sub, compile_sub, generate_sub, test_sub,
                          rpc_sub)
    # --models, --exclude
    _add_selection_arguments(run_sub, compile_sub, generate_sub, test_sub,
                             archive_sub)
    # --full-refresh
    _add_table_mutability_arguments(run_sub, compile_sub)

    _build_seed_subparser(subs, base_subparser)
    _build_docs_serve_subparser(docs_subs, base_subparser)
    _build_source_snapshot_freshness_subparser(source_subs, base_subparser)

    sub = subs.add_parser(
        'run-operation',
        parents=[base_subparser],
        help="""
            (beta) Run the named macro with any supplied arguments. This
            subcommand is unstable and subject to change in a future release
            of dbt. Please use it with caution"""
    )
    sub.add_argument(
        '--macro',
        required=True,
        help="""
            Specify the macro to invoke. dbt will call this macro with the
            supplied arguments and then exit"""
    )
    sub.add_argument(
        '--args',
        type=str,
        default='{}',
        help="""
            Supply arguments to the macro. This dictionary will be mapped
            to the keyword arguments defined in the selected macro. This
            argument should be a YAML string, eg. '{my_variable: my_value}'"""
    )
    sub.set_defaults(cls=run_operation_task.RunOperationTask,
                     which='run-operation')

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)
    parsed.profiles_dir = os.path.expanduser(parsed.profiles_dir)

    if not hasattr(parsed, 'which'):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed
