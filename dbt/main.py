from dbt.logger import initialize_logger, GLOBAL_LOGGER as logger

import argparse
import os.path
import sys
import traceback

import dbt.version
import dbt.flags as flags
import dbt.project as project
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

import dbt.tracking
import dbt.config as config
import dbt.ui.printer
import dbt.compat
import dbt.deprecations

from dbt.utils import ExitCodes

PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""


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

        logger.debug(traceback.format_exc())
        exit_code = ExitCodes.UnhandledError

    sys.exit(exit_code)


# here for backwards compatibility
def handle(args):
    res, success = handle_and_check(args)
    return res


def handle_and_check(args):
    parsed = parse_args(args)

    # this needs to happen after args are parsed so we can determine the
    # correct profiles.yml file
    profile_config = config.read_config(parsed.profiles_dir)
    if not config.send_anonymous_usage_stats(profile_config):
        dbt.tracking.do_not_track()
    else:
        dbt.tracking.initialize_tracking()

    if dbt.config.colorize_output(profile_config):
        dbt.ui.printer.use_colors()

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
    proj = None

    if parsed.which == 'init':
        # bypass looking for a project file if we're running `dbt init`
        task = parsed.cls(args=parsed)
    else:
        nearest_project_dir = get_nearest_project_dir()
        if nearest_project_dir is None:
            raise RuntimeError(
                "fatal: Not a dbt project (or any of the parent directories). "
                "Missing dbt_project.yml file"
            )

        os.chdir(nearest_project_dir)

        res = invoke_dbt(parsed)
        if res is None:
            raise RuntimeError("Could not run dbt")
        else:
            task, proj = res

    log_path = None

    if proj is not None:
        log_path = proj.get('log-path', 'logs')

    initialize_logger(parsed.debug, log_path)
    logger.debug("Tracking: {}".format(dbt.tracking.active_user.state()))

    dbt.tracking.track_invocation_start(project=proj, args=parsed)

    results = run_from_task(task, proj, parsed)

    return task, results


def run_from_task(task, proj, parsed_args):
    result = None
    try:
        result = task.run()
        dbt.tracking.track_invocation_end(
            project=proj, args=parsed_args, result_type="ok"
        )
    except (dbt.exceptions.NotImplementedException,
            dbt.exceptions.FailedToConnectException) as e:
        logger.info('ERROR: {}'.format(e))
        dbt.tracking.track_invocation_end(
            project=proj, args=parsed_args, result_type="error"
        )
    except Exception as e:
        dbt.tracking.track_invocation_end(
            project=proj, args=parsed_args, result_type="error"
        )
        raise

    return result


def invoke_dbt(parsed):
    task = None
    proj = None

    try:
        proj = project.read_project(
            'dbt_project.yml',
            parsed.profiles_dir,
            validate=False,
            profile_to_load=parsed.profile,
            args=parsed
        )
        proj.validate()
    except project.DbtProjectError as e:
        logger.info("Encountered an error while reading the project:")
        logger.info(dbt.compat.to_string(e))

        all_profiles = project.read_profiles(parsed.profiles_dir).keys()

        if len(all_profiles) > 0:
            logger.info("Defined profiles:")
            for profile in all_profiles:
                logger.info(" - {}".format(profile))
        else:
            logger.info("There are no profiles defined in your "
                        "profiles.yml file")

        logger.info(PROFILES_HELP_MESSAGE)

        dbt.tracking.track_invalid_invocation(
            project=proj,
            args=parsed,
            result_type="invalid_profile")

        return None
    except project.DbtProfileError as e:
        logger.info("Encountered an error while reading profiles:")
        logger.info("  ERROR {}".format(str(e)))

        dbt.tracking.track_invalid_invocation(
            project=proj,
            args=parsed,
            result_type="invalid_profile")

        return None

    if parsed.target is not None:
        targets = proj.cfg.get('outputs', {}).keys()
        if parsed.target in targets:
            proj.cfg['target'] = parsed.target
            # make sure we update the target if this is overriden on the cli
            proj.compile_and_update_target()
        else:
            logger.info("Encountered an error while reading the project:")
            logger.info("  ERROR Specified target {} is not a valid option "
                        "for profile {}"
                        .format(parsed.target, proj.profile_to_load))
            logger.info("Valid targets are: {}".format(
                ', '.join(targets)))
            dbt.tracking.track_invalid_invocation(
                project=proj,
                args=parsed,
                result_type="invalid_target")

            return None

    proj.log_warnings()

    flags.NON_DESTRUCTIVE = getattr(proj.args, 'non_destructive', False)

    arg_drop_existing = getattr(proj.args, 'drop_existing', False)
    arg_full_refresh = getattr(proj.args, 'full_refresh', False)

    if arg_drop_existing:
        dbt.deprecations.warn('drop-existing')
        flags.FULL_REFRESH = True
    elif arg_full_refresh:
        flags.FULL_REFRESH = True

    logger.debug("running dbt with arguments %s", parsed)

    task = parsed.cls(args=parsed, project=proj)

    return task, proj


def parse_args(args):
    p = argparse.ArgumentParser(
        prog='dbt: data build tool',
        formatter_class=argparse.RawTextHelpFormatter)

    p.add_argument(
        '--version',
        action='version',
        version=dbt.version.get_version_information(),
        help="Show version information")

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

    subs = p.add_subparsers()

    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        '--profiles-dir',
        default=project.default_profiles_dir,
        type=str,
        help="""
        Which directory to look in for the profiles.yml file. Default = {}
        """.format(project.default_profiles_dir)
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

    sub = subs.add_parser('init', parents=[base_subparser])
    sub.add_argument('project_name', type=str, help='Name of the new project')
    sub.set_defaults(cls=init_task.InitTask, which='init')

    sub = subs.add_parser('clean', parents=[base_subparser])
    sub.set_defaults(cls=clean_task.CleanTask, which='clean')

    sub = subs.add_parser('debug', parents=[base_subparser])
    sub.add_argument(
        '--config-dir',
        action='store_true',
        help="""
        If specified, DBT will show path information for this project
        """
    )
    sub.set_defaults(cls=debug_task.DebugTask, which='debug')

    sub = subs.add_parser('deps', parents=[base_subparser])
    sub.set_defaults(cls=deps_task.DepsTask, which='deps')

    sub = subs.add_parser('archive', parents=[base_subparser])
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

    run_sub = subs.add_parser('run', parents=[base_subparser])
    run_sub.set_defaults(cls=run_task.RunTask, which='run')

    compile_sub = subs.add_parser('compile', parents=[base_subparser])
    compile_sub.set_defaults(cls=compile_task.CompileTask, which='compile')

    for sub in [run_sub, compile_sub]:
        sub.add_argument(
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

    seed_sub = subs.add_parser('seed', parents=[base_subparser])
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

    docs_sub = subs.add_parser('docs', parents=[base_subparser])
    docs_subs = docs_sub.add_subparsers()
    # it might look like docs_sub is the correct parents entry, but that
    # will cause weird errors about 'conflicting option strings'.
    generate_sub = docs_subs.add_parser('generate', parents=[base_subparser])
    generate_sub.set_defaults(cls=generate_task.GenerateTask,
                              which='generate')

    sub = subs.add_parser('test', parents=[base_subparser])
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

    return parsed
