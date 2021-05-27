from typing import List
from dbt.logger import GLOBAL_LOGGER as logger, log_cache_events, log_manager

import argparse
import os.path
import sys
import traceback
from contextlib import contextmanager
from pathlib import Path

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
import dbt.task.snapshot as snapshot_task
import dbt.task.generate as generate_task
import dbt.task.serve as serve_task
import dbt.task.freshness as freshness_task
import dbt.task.run_operation as run_operation_task
import dbt.task.parse as parse_task
from dbt.profiler import profiler
from dbt.task.list import ListTask
from dbt.task.rpc.server import RPCServerTask
from dbt.adapters.factory import reset_adapters, cleanup_connections

import dbt.tracking

from dbt.utils import ExitCodes
from dbt.config import PROFILES_DIR, read_user_config
from dbt.exceptions import RuntimeException, InternalException


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
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        formatter = argparse.RawTextHelpFormatter(prog=parser.prog)
        formatter.add_text(dbt.version.get_version_information())
        parser.exit(message=formatter.format_help())


class DBTArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register('action', 'dbtversion', DBTVersion)

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
        if not name.startswith('--'):
            raise InternalException(
                'cannot handle optional argument without "--" prefix: '
                f'got "{name}"'
            )
        if dest is None:
            dest_name = name[2:].replace('-', '_')
        else:
            dest_name = dest

        if no_name is None:
            no_name = f'--no-{name[2:]}'

        mutex_group.add_argument(
            name,
            action='store_const',
            const=True,
            dest=dest_name,
            default=default,
            help=enable_help,
        )

        mutex_group.add_argument(
            f'--no-{name[2:]}',
            action='store_const',
            const=False,
            dest=dest_name,
            default=default,
            help=disable_help,
        )

        return mutex_group


class RPCArgumentParser(DBTArgumentParser):
    def exit(self, status=0, message=None):
        if status == 0:
            return
        else:
            raise TypeError(message)


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
            logger.info("ctrl-c")
            exit_code = ExitCodes.UnhandledError.value

        # This can be thrown by eg. argparse
        except SystemExit as e:
            exit_code = e.code

        except BaseException as e:
            logger.warning("Encountered an error:")
            logger.warning(str(e))

            if log_manager.initialized:
                logger.debug(traceback.format_exc())
            elif not isinstance(e, RuntimeException):
                # if it did not come from dbt proper and the logger is not
                # initialized (so there's no safe path to log to), log the
                # stack trace at error level.
                logger.error(traceback.format_exc())
            exit_code = ExitCodes.UnhandledError.value

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
    cfg = read_user_config(parsed.profiles_dir)
    cfg.set_values(parsed.profiles_dir)


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

        # we've parsed the args - we can now decide if we're debug or not
        if parsed.debug:
            log_manager.set_debug()

        profiler_enabled = False

        if parsed.record_timing_info:
            profiler_enabled = True

        with profiler(
            enable=profiler_enabled,
            outfile=parsed.record_timing_info
        ):

            initialize_config_values(parsed)

            with adapter_management():

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
    flags.set_from_args(parsed)

    parsed.cls.pre_init_hook(parsed)
    # we can now use the logger for stdout

    logger.info("Running with dbt{}".format(dbt.version.installed))

    # this will convert DbtConfigErrors into RuntimeExceptions
    task = parsed.cls.from_args(args=parsed)
    logger.debug("running dbt with arguments {parsed}", parsed=str(parsed))

    log_path = None
    if task.config is not None:
        log_path = getattr(task.config, 'log_path', None)
    # we can finally set the file logger up
    log_manager.set_path(log_path)
    if dbt.tracking.active_user is not None:  # mypy appeasement, always true
        logger.debug("Tracking: {}".format(dbt.tracking.active_user.state()))

    results = None

    with track_run(task):
        results = task.run()

    return task, results


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--project-dir',
        default=None,
        type=str,
        help='''
        Which directory to look in for the dbt_project.yml file.
        Default is the current working directory and its parents.
        '''
    )

    base_subparser.add_argument(
        '--profiles-dir',
        default=PROFILES_DIR,
        type=str,
        help='''
        Which directory to look in for the profiles.yml file. Default = {}
        '''.format(PROFILES_DIR)
    )

    base_subparser.add_argument(
        '--profile',
        required=False,
        type=str,
        help='''
        Which profile to load. Overrides setting in dbt_project.yml.
        '''
    )

    base_subparser.add_argument(
        '-t',
        '--target',
        default=None,
        type=str,
        help='''
        Which target to load for the given profile
        ''',
    )

    base_subparser.add_argument(
        '--vars',
        type=str,
        default='{}',
        help='''
        Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}'
        '''
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
        help='''
        If set, bypass the adapter-level cache of database state
        ''',
    )

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser


def _build_docs_subparser(subparsers, base_subparser):
    docs_sub = subparsers.add_parser(
        'docs',
        help='''
        Generate or serve the documentation website for your project.
        '''
    )
    return docs_sub


def _build_source_subparser(subparsers, base_subparser):
    source_sub = subparsers.add_parser(
        'source',
        help='''
        Manage your project's sources
        ''',
    )
    return source_sub


def _build_init_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'init',
        parents=[base_subparser],
        help='''
        Initialize a new DBT project.
        '''
    )
    sub.add_argument(
        'project_name',
        type=str,
        help='''
        Name of the new project
        ''',
    )
    sub.add_argument(
        '--adapter',
        default='redshift',
        type=str,
        help='''
        Write sample profiles.yml for which adapter
        ''',
    )
    sub.set_defaults(cls=init_task.InitTask, which='init', rpc_method=None)
    return sub


def _build_clean_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'clean',
        parents=[base_subparser],
        help='''
        Delete all folders in the clean-targets list
        (usually the dbt_modules and target directories.)
        '''
    )
    sub.set_defaults(cls=clean_task.CleanTask, which='clean', rpc_method=None)
    return sub


def _build_debug_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'debug',
        parents=[base_subparser],
        help='''
        Show some helpful information about dbt for debugging.

        Not to be confused with the --debug option which increases verbosity.
        '''
    )
    sub.add_argument(
        '--config-dir',
        action='store_true',
        help='''
        If specified, DBT will show path information for this project
        '''
    )
    _add_version_check(sub)
    sub.set_defaults(cls=debug_task.DebugTask, which='debug', rpc_method=None)
    return sub


def _build_deps_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'deps',
        parents=[base_subparser],
        help='''
        Pull the most recent version of the dependencies listed in packages.yml
        '''
    )
    sub.set_defaults(cls=deps_task.DepsTask, which='deps', rpc_method='deps')
    return sub


def _build_snapshot_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'snapshot',
        parents=[base_subparser],
        help='''
        Execute snapshots defined in your project
        ''',
    )
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help='''
        Specify number of threads to use while snapshotting tables.
        Overrides settings in profiles.yml.
        '''
    )
    sub.set_defaults(cls=snapshot_task.SnapshotTask, which='snapshot',
                     rpc_method='snapshot')
    return sub


def _add_defer_argument(*subparsers):
    for sub in subparsers:
        sub.add_optional_argument_inverse(
            '--defer',
            enable_help='''
            If set, defer to the state variable for resolving unselected nodes.
            ''',
            disable_help='''
            If set, do not defer to the state variable for resolving unselected
            nodes.
            ''',
            default=flags.DEFER_MODE,
        )


def _build_run_subparser(subparsers, base_subparser):
    run_sub = subparsers.add_parser(
        'run',
        parents=[base_subparser],
        help='''
        Compile SQL and execute against the current target database.
        '''
    )
    run_sub.add_argument(
        '-x',
        '--fail-fast',
        action='store_true',
        help='''
        Stop execution upon a first failure.
        '''
    )

    run_sub.set_defaults(cls=run_task.RunTask, which='run', rpc_method='run')
    return run_sub


def _build_compile_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'compile',
        parents=[base_subparser],
        help='''
        Generates executable SQL from source, model, test, and analysis files.
        Compiled SQL files are written to the target/ directory.
        '''
    )
    sub.set_defaults(cls=compile_task.CompileTask, which='compile',
                     rpc_method='compile')
    sub.add_argument('--parse-only', action='store_true')
    return sub


def _build_parse_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'parse',
        parents=[base_subparser],
        help='''
        Parsed the project and provides information on performance
        '''
    )
    sub.set_defaults(cls=parse_task.ParseTask, which='parse',
                     rpc_method='parse')
    sub.add_argument('--write-manifest', action='store_true')
    sub.add_argument('--compile', action='store_true')
    return sub


def _build_docs_generate_subparser(subparsers, base_subparser):
    # it might look like docs_sub is the correct parents entry, but that
    # will cause weird errors about 'conflicting option strings'.
    generate_sub = subparsers.add_parser('generate', parents=[base_subparser])
    generate_sub.set_defaults(cls=generate_task.GenerateTask,
                              which='generate', rpc_method='docs.generate')
    generate_sub.add_argument(
        '--no-compile',
        action='store_false',
        dest='compile',
        help='''
        Do not run "dbt compile" as part of docs generation
        ''',
    )
    return generate_sub


def _add_models_argument(sub, help_override=None, **kwargs):
    help_str = '''
        Specify the models to include.
    '''
    if help_override is not None:
        help_str = help_override
    sub.add_argument(
        '-m',
        '--models',
        dest='models',
        nargs='+',
        help=help_str,
        **kwargs
    )


def _add_select_argument(sub, dest='models', help_override=None, **kwargs):
    help_str = '''
        Specify the nodes to include.
    '''
    if help_override is not None:
        help_str = help_override

    sub.add_argument(
        '-s',
        '--select',
        dest=dest,
        nargs='+',
        help=help_str,
        **kwargs
    )


def _add_common_selector_arguments(sub):
    sub.add_argument(
        '--exclude',
        required=False,
        nargs='+',
        help='''
            Specify the models to exclude.
        ''',
    )
    sub.add_argument(
        '--selector',
        dest='selector_name',
        metavar='SELECTOR_NAME',
        help='''
        The selector name to use, as defined in selectors.yml
        '''
    )
    sub.add_argument(
        '--state',
        help='''
        If set, use the given directory as the source for json files to
        compare with this project.
        ''',
        type=Path,
        default=flags.ARTIFACT_STATE_PATH,
    )


def _add_selection_arguments(*subparsers, **kwargs):
    models_name = kwargs.get('models_name', 'models')
    for sub in subparsers:
        if models_name == 'models':
            _add_models_argument(sub)
        elif models_name == 'select':
            # these still get stored in 'models', so they present the same
            # interface to the task
            _add_select_argument(sub)
        else:
            raise InternalException(f'Unknown models style {models_name}')
        _add_common_selector_arguments(sub)


def _add_table_mutability_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            '--full-refresh',
            action='store_true',
            help='''
            If specified, DBT will drop incremental models and
            fully-recalculate the incremental table from the model definition.
            '''
        )


def _add_version_check(sub):
    sub.add_argument(
        '--no-version-check',
        dest='version_check',
        action='store_false',
        help='''
        If set, skip ensuring dbt's version matches the one specified in
        the dbt_project.yml file ('require-dbt-version')
        '''
    )


def _add_common_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            '--threads',
            type=int,
            required=False,
            help='''
            Specify number of threads to use while executing models. Overrides
            settings in profiles.yml.
            '''
        )
        _add_version_check(sub)


def _build_seed_subparser(subparsers, base_subparser):
    seed_sub = subparsers.add_parser(
        'seed',
        parents=[base_subparser],
        help='''
        Load data from csv files into your data warehouse.
        ''',
    )
    seed_sub.add_argument(
        '--full-refresh',
        action='store_true',
        help='''
        Drop existing seed tables and recreate them
        ''',
    )
    seed_sub.add_argument(
        '--show',
        action='store_true',
        help='''
        Show a sample of the loaded data in the terminal
        '''
    )
    seed_sub.set_defaults(cls=seed_task.SeedTask, which='seed',
                          rpc_method='seed')
    return seed_sub


def _build_docs_serve_subparser(subparsers, base_subparser):
    serve_sub = subparsers.add_parser('serve', parents=[base_subparser])
    serve_sub.add_argument(
        '--port',
        default=8080,
        type=int,
        help='''
        Specify the port number for the docs server.
        '''
    )
    serve_sub.add_argument(
        '--no-browser',
        dest='open_browser',
        action='store_false',
    )
    serve_sub.set_defaults(cls=serve_task.ServeTask, which='serve',
                           rpc_method=None)
    return serve_sub


def _build_test_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'test',
        parents=[base_subparser],
        help='''
        Runs tests on data in deployed models. Run this after `dbt run`
        '''
    )
    sub.add_argument(
        '--data',
        action='store_true',
        help='''
        Run data tests defined in "tests" directory.
        '''
    )
    sub.add_argument(
        '--schema',
        action='store_true',
        help='''
        Run constraint validations from schema.yml files
        '''
    )
    sub.add_argument(
        '-x',
        '--fail-fast',
        action='store_true',
        help='''
        Stop execution upon a first test failure.
        '''
    )
    sub.add_argument(
        '--store-failures',
        action='store_true',
        help='''
        Store test results (failing rows) in the database
        '''
    )

    sub.set_defaults(cls=test_task.TestTask, which='test', rpc_method='test')
    return sub


def _build_source_snapshot_freshness_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'snapshot-freshness',
        parents=[base_subparser],
        help='''
        Snapshots the current freshness of the project's sources
        ''',
    )
    sub.add_argument(
        '-s',
        '--select',
        required=False,
        nargs='+',
        help='''
        Specify the sources to snapshot freshness
        ''',
        dest='selected'
    )
    sub.add_argument(
        '-o',
        '--output',
        required=False,
        help='''
        Specify the output path for the json report. By default, outputs to
        target/sources.json
        '''
    )
    sub.add_argument(
        '--threads',
        type=int,
        required=False,
        help='''
        Specify number of threads to use. Overrides settings in profiles.yml
        '''
    )
    sub.set_defaults(
        cls=freshness_task.FreshnessTask,
        which='snapshot-freshness',
        rpc_method='snapshot-freshness',
    )
    return sub


def _build_rpc_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'rpc',
        parents=[base_subparser],
        help='''
        Start a json-rpc server
        ''',
    )
    sub.add_argument(
        '--host',
        default='0.0.0.0',
        help='''
        Specify the host to listen on for the rpc server.
        ''',
    )
    sub.add_argument(
        '--port',
        default=8580,
        type=int,
        help='''
        Specify the port number for the rpc server.
        ''',
    )
    sub.set_defaults(cls=RPCServerTask, which='rpc', rpc_method=None)
    # the rpc task does a 'compile', so we need these attributes to exist, but
    # we don't want users to be allowed to set them.
    sub.set_defaults(models=None, exclude=None)
    return sub


def _build_list_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'list',
        parents=[base_subparser],
        help='''
        List the resources in your project
        ''',
        aliases=['ls'],
    )
    sub.set_defaults(cls=ListTask, which='list', rpc_method=None)
    resource_values: List[str] = [
        str(s) for s in ListTask.ALL_RESOURCE_VALUES
    ] + ['default', 'all']
    sub.add_argument('--resource-type',
                     choices=resource_values,
                     action='append',
                     default=[],
                     dest='resource_types')
    sub.add_argument('--output',
                     choices=['json', 'name', 'path', 'selector'],
                     default='selector')

    _add_models_argument(
        sub,
        help_override='''
        Specify the models to select and set the resource-type to 'model'.
        Mutually exclusive with '--select' (or '-s') and '--resource-type'
        ''',
        metavar='SELECTOR',
        required=False
    )
    _add_select_argument(
        sub,
        dest='select',
        metavar='SELECTOR',
        required=False,
    )
    _add_common_selector_arguments(sub)

    return sub


def _build_run_operation_subparser(subparsers, base_subparser):
    sub = subparsers.add_parser(
        'run-operation',
        parents=[base_subparser],
        help='''
        Run the named macro with any supplied arguments.
        '''
    )
    sub.add_argument(
        'macro',
        help='''
        Specify the macro to invoke. dbt will call this macro with the supplied
        arguments and then exit
        ''',
    )
    sub.add_argument(
        '--args',
        type=str,
        default='{}',
        help='''
        Supply arguments to the macro. This dictionary will be mapped to the
        keyword arguments defined in the selected macro. This argument should
        be a YAML string, eg. '{my_variable: my_value}'
        '''
    )
    sub.set_defaults(cls=run_operation_task.RunOperationTask,
                     which='run-operation', rpc_method='run-operation')
    return sub


def parse_args(args, cls=DBTArgumentParser):
    p = cls(
        prog='dbt',
        description='''
        An ELT tool for managing your SQL transformations and data models.
        For more documentation on these commands, visit: docs.getdbt.com
        ''',
        epilog='''
        Specify one of these sub-commands and you can find more help from
        there.
        '''
    )

    p.add_argument(
        '--version',
        action='dbtversion',
        help='''
        Show version information
        ''')

    p.add_argument(
        '-r',
        '--record-timing-info',
        default=None,
        type=str,
        help='''
        When this option is passed, dbt will output low-level timing stats to
        the specified file. Example: `--record-timing-info output.profile`
        '''
    )

    p.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='''
        Display debug logging during dbt execution. Useful for debugging and
        making bug reports.
        '''
    )

    p.add_argument(
        '--log-format',
        choices=['text', 'json', 'default'],
        default='default',
        help='''Specify the log format, overriding the command's default.'''
    )

    p.add_argument(
        '--no-write-json',
        action='store_false',
        dest='write_json',
        help='''
        If set, skip writing the manifest and run_results.json files to disk
        '''
    )
    colors_flag = p.add_mutually_exclusive_group()
    colors_flag.add_argument(
        '--use-colors',
        action='store_const',
        const=True,
        dest='use_colors',
        help='''
        Colorize the output DBT prints to the terminal. Output is colorized by
        default and may also be set in a profile or at the command line.
        Mutually exclusive with --no-use-colors
        '''
    )
    colors_flag.add_argument(
        '--no-use-colors',
        action='store_const',
        const=False,
        dest='use_colors',
        help='''
        Do not colorize the output DBT prints to the terminal. Output is
        colorized by default and may also be set in a profile or at the
        command line.
        Mutually exclusive with --use-colors
        '''
    )

    p.add_argument(
        '-S',
        '--strict',
        action='store_true',
        help='''
        Run schema validations at runtime. This will surface bugs in dbt, but
        may incur a performance penalty.
        '''
    )

    p.add_argument(
        '--warn-error',
        action='store_true',
        help='''
        If dbt would normally warn, instead raise an exception. Examples
        include --models that selects nothing, deprecations, configurations
        with no associated models, invalid test configurations, and missing
        sources/refs in tests.
        '''
    )

    p.add_optional_argument_inverse(
        '--partial-parse',
        enable_help='''
        Allow for partial parsing by looking for and writing to a pickle file
        in the target directory. This overrides the user configuration file.

        WARNING: This can result in unexpected behavior if you use env_var()!
        ''',
        disable_help='''
        Disallow partial parsing. This overrides the user configuration file.
        ''',
    )

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
    # TODO remove?
    p.add_argument(
        '--test-new-parser',
        action='store_true',
        help=argparse.SUPPRESS
    )

    # if set, will use the tree-sitter-jinja2 parser and extractor instead of
    # jinja rendering when possible.
    p.add_argument(
        '--use-experimental-parser',
        action='store_true',
        help='''
        Uses an experimental parser to extract jinja values.
        '''
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

    snapshot_sub = _build_snapshot_subparser(subs, base_subparser)
    rpc_sub = _build_rpc_subparser(subs, base_subparser)
    run_sub = _build_run_subparser(subs, base_subparser)
    compile_sub = _build_compile_subparser(subs, base_subparser)
    parse_sub = _build_parse_subparser(subs, base_subparser)
    generate_sub = _build_docs_generate_subparser(docs_subs, base_subparser)
    test_sub = _build_test_subparser(subs, base_subparser)
    seed_sub = _build_seed_subparser(subs, base_subparser)
    # --threads, --no-version-check
    _add_common_arguments(run_sub, compile_sub, generate_sub, test_sub,
                          rpc_sub, seed_sub, parse_sub)
    # --models, --exclude
    # list_sub sets up its own arguments.
    _add_selection_arguments(run_sub, compile_sub, generate_sub, test_sub)
    _add_selection_arguments(snapshot_sub, seed_sub, models_name='select')
    # --defer
    _add_defer_argument(run_sub, test_sub)
    # --full-refresh
    _add_table_mutability_arguments(run_sub, compile_sub)

    _build_docs_serve_subparser(docs_subs, base_subparser)
    _build_source_snapshot_freshness_subparser(source_subs, base_subparser)
    _build_run_operation_subparser(subs, base_subparser)

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if hasattr(parsed, 'profiles_dir'):
        parsed.profiles_dir = os.path.abspath(parsed.profiles_dir)

    if getattr(parsed, 'project_dir', None) is not None:
        expanded_user = os.path.expanduser(parsed.project_dir)
        parsed.project_dir = os.path.abspath(expanded_user)

    if not hasattr(parsed, 'which'):
        # the user did not provide a valid subcommand. trigger the help message
        # and exit with a error
        p.print_help()
        p.exit(1)

    return parsed
