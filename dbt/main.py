from dbt.logger import initialize_logger, GLOBAL_LOGGER as logger, \
    logger_initialized, log_cache_events

import argparse
import os.path
import sys
import traceback

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
from dbt.adapters.factory import reset_adapters

import dbt.tracking
import dbt.ui.printer
import dbt.compat
import dbt.deprecations
import dbt.profiler

from dbt.utils import ExitCodes
from dbt.config import Project, RuntimeConfig, DbtProjectError, \
    DbtProfileError, PROFILES_DIR, read_config, \
    send_anonymous_usage_stats, colorize_output, read_profiles
from dbt.exceptions import DbtProfileError, DbtProfileError, RuntimeException


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

    except KeyboardInterrupt as e:
        logger.info("ctrl-c")
        exit_code = ExitCodes.UnhandledError

    # This can be thrown by eg. argparse
    except SystemExit as e:
        exit_code = e.code

    except BaseException as e:
        logger.info("Encountered an error:")
        logger.info(str(e))

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


def handle_and_check(args):
    parsed = parse_args(args)
    profiler_enabled = False

    if parsed.record_timing_info:
        profiler_enabled = True

    with dbt.profiler.profiler(
        enable=profiler_enabled,
        outfile=parsed.record_timing_info
    ):
        # this needs to happen after args are parsed so we can determine the
        # correct profiles.yml file
        profile_config = read_config(parsed.profiles_dir)
        if not send_anonymous_usage_stats(profile_config):
            dbt.tracking.do_not_track()
        else:
            dbt.tracking.initialize_tracking()

        if colorize_output(profile_config):
            dbt.ui.printer.use_colors()

        reset_adapters()

        try:
            task, res = run_from_args(parsed)
        finally:
            dbt.tracking.flush()

        success = task.interpret_results(res)

        return res, success


def get_nearest_project_dir():
    root_path = os.path.abspath(os.sep)
    cwd = os.getcwd()

    while cwd != root_path:
        project_file = os.path.join(cwd, "dbt_project.yml")
        if os.path.exists(project_file):
            return cwd
        cwd = os.path.dirname(cwd)

    return None


def run_from_args(parsed):
    task = None
    cfg = None

    if parsed.which in ('init', 'debug'):
        # bypass looking for a project file if we're running `dbt init` or
        # `dbt debug`
        task = parsed.cls(args=parsed)
    else:
        nearest_project_dir = get_nearest_project_dir()
        if nearest_project_dir is None:
            raise RuntimeException(
                "fatal: Not a dbt project (or any of the parent directories). "
                "Missing dbt_project.yml file"
            )

        os.chdir(nearest_project_dir)

        res = invoke_dbt(parsed)
        if res is None:
            raise RuntimeException("Could not run dbt")
        else:
            task, cfg = res

    log_path = None

    if cfg is not None:
        log_path = cfg.log_path

    initialize_logger(parsed.debug, log_path)
    logger.debug("Tracking: {}".format(dbt.tracking.active_user.state()))

    dbt.tracking.track_invocation_start(config=cfg, args=parsed)

    results = run_from_task(task, cfg, parsed)

    return task, results


def run_from_task(task, cfg, parsed_args):
    result = None
    try:
        result = task.run()
        dbt.tracking.track_invocation_end(
            config=cfg, args=parsed_args, result_type="ok"
        )
    except (dbt.exceptions.NotImplementedException,
            dbt.exceptions.FailedToConnectException) as e:
        logger.info('ERROR: {}'.format(e))
        dbt.tracking.track_invocation_end(
            config=cfg, args=parsed_args, result_type="error"
        )
    except Exception as e:
        dbt.tracking.track_invocation_end(
            config=cfg, args=parsed_args, result_type="error"
        )
        raise

    return result


def invoke_dbt(parsed):
    task = None
    cfg = None

    log_cache_events(getattr(parsed, 'log_cache_events', False))

    try:
        if parsed.which in {'deps', 'clean'}:
            # deps doesn't need a profile, so don't require one.
            cfg = Project.from_current_directory(getattr(parsed, 'vars', '{}'))
        elif parsed.which != 'debug':
            # for debug, we will attempt to load the various configurations as
            # part of the task, so just leave cfg=None.
            cfg = RuntimeConfig.from_args(parsed)
    except DbtProjectError as e:
        logger.info("Encountered an error while reading the project:")
        logger.info(dbt.compat.to_string(e))

        dbt.tracking.track_invalid_invocation(
            config=cfg,
            args=parsed,
            result_type=e.result_type)

        return None
    except DbtProfileError as e:
        logger.info("Encountered an error while reading profiles:")
        logger.info("  ERROR {}".format(str(e)))

        all_profiles = read_profiles(parsed.profiles_dir).keys()

        if len(all_profiles) > 0:
            logger.info("Defined profiles:")
            for profile in all_profiles:
                logger.info(" - {}".format(profile))
        else:
            logger.info("There are no profiles defined in your "
                        "profiles.yml file")

        logger.info(PROFILES_HELP_MESSAGE)

        dbt.tracking.track_invalid_invocation(
            config=cfg,
            args=parsed,
            result_type=e.result_type)

        return None

    flags.NON_DESTRUCTIVE = getattr(parsed, 'non_destructive', False)
    flags.USE_CACHE = getattr(parsed, 'use_cache', True)

    arg_drop_existing = getattr(parsed, 'drop_existing', False)
    arg_full_refresh = getattr(parsed, 'full_refresh', False)

    if arg_drop_existing:
        dbt.deprecations.warn('drop-existing')
        flags.FULL_REFRESH = True
    elif arg_full_refresh:
        flags.FULL_REFRESH = True

    logger.debug("running dbt with arguments %s", parsed)

    task = parsed.cls(args=parsed, config=cfg)

    return task, cfg


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

    subs = p.add_subparsers(title="Available sub-commands")

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

    sub = subs.add_parser(
            'init',
            parents=[base_subparser],
            help="Initialize a new DBT project.")
    sub.add_argument('project_name', type=str, help='Name of the new project')
    sub.set_defaults(cls=init_task.InitTask, which='init')

    sub = subs.add_parser(
        'clean',
        parents=[base_subparser],
        help="Delete all folders in the clean-targets list"
        "\n(usually the dbt_modules and target directories.)")
    sub.set_defaults(cls=clean_task.CleanTask, which='clean')

    sub = subs.add_parser(
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

    sub = subs.add_parser(
        'deps',
        parents=[base_subparser],
        help="Pull the most recent version of the dependencies "
        "listed in packages.yml")
    sub.set_defaults(cls=deps_task.DepsTask, which='deps')

    sub = subs.add_parser(
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

    run_sub = subs.add_parser(
        'run',
        parents=[base_subparser],
        help="Compile SQL and execute against the current "
        "target database.")
    run_sub.set_defaults(cls=run_task.RunTask, which='run')

    compile_sub = subs.add_parser(
        'compile',
        parents=[base_subparser],
        help="Generates executable SQL from source model, test, and"
        "analysis files. \nCompiled SQL files are written to the target/"
        "directory.")
    compile_sub.set_defaults(cls=compile_task.CompileTask, which='compile')

    docs_sub = subs.add_parser(
        'docs',
        parents=[base_subparser],
        help="Generate or serve the documentation "
        "website for your project.")
    docs_subs = docs_sub.add_subparsers()
    # it might look like docs_sub is the correct parents entry, but that
    # will cause weird errors about 'conflicting option strings'.
    generate_sub = docs_subs.add_parser('generate', parents=[base_subparser])
    generate_sub.set_defaults(cls=generate_task.GenerateTask,
                              which='generate')
    generate_sub.add_argument(
        '--no-compile',
        action='store_false',
        dest='compile',
        help='Do not run "dbt compile" as part of docs generation'
    )

    for sub in [run_sub, compile_sub, generate_sub]:
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
            '--non-destructive',
            action='store_true',
            help="""
            If specified, DBT will not drop views. Tables will be truncated
            instead of dropped.
            """
        )
        sub.add_argument(
            '--full-refresh',
            action='store_true',
            help="""
            If specified, DBT will drop incremental models and
            fully-recalculate the incremental table from the model definition.
            """)

    seed_sub = subs.add_parser(
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

    serve_sub = docs_subs.add_parser('serve', parents=[base_subparser])
    serve_sub.add_argument(
        '--port',
        default=8080,
        type=int,
        help='Specify the port number for the docs server.'
    )
    serve_sub.set_defaults(cls=serve_task.ServeTask,
                           which='serve')

    sub = subs.add_parser(
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
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help="""
        Specify number of threads to use while executing tests. Overrides
        settings in profiles.yml
        """
    )
    sub.add_argument(
        '--models',
        required=False,
        nargs='+',
        help="""
        Specify the models to test.
        """
    )
    sub.add_argument(
        '--exclude',
        required=False,
        nargs='+',
        help="""
        Specify the models to exclude from testing.
        """
    )

    sub.set_defaults(cls=test_task.TestTask, which='test')

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if not hasattr(parsed, 'which'):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed
