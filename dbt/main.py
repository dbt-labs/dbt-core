
from dbt.logger import getLogger

import argparse
import os.path
import sys
import re

import dbt.version
import dbt.project as project
import dbt.task.run as run_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.clean as clean_task
import dbt.task.deps as deps_task
import dbt.task.init as init_task
import dbt.task.seed as seed_task
import dbt.task.test as test_task
import dbt.tracking


def is_opted_out():
    profiles = project.read_profiles()

    if profiles is None or profiles.get("config") is None:
        return False
    elif profiles['config'].get("send_anonymous_usage_stats") == False:
        return True
    else:
        return False

#import dbt.task.hosted.hosted as hosted_task
import dbt.task.hosted.auth as hosted_auth_task
import dbt.task.hosted.initialize as hosted_initialize_task
import dbt.task.hosted.push as hosted_push_task
import dbt.task.hosted.configure as hosted_configure_task

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    if is_opted_out():
        dbt.tracking.do_not_track()

    try:
        handle(args)
        dbt.tracking.flush()

    except RuntimeError as e:
        print("Encountered an error:")
        print(str(e))
        sys.exit(1)

def handle(args):
    p = argparse.ArgumentParser(prog='dbt: data build tool', formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('--version', action='version', version=dbt.version.get_version_information(), help="Show version information")
    subs = p.add_subparsers()

    base_subparser = argparse.ArgumentParser(add_help=False)
    base_subparser.add_argument('--profile', default=["user"], nargs='+', type=str, help='Which profile to load')
    base_subparser.add_argument('--target', default=None, type=str, help='Which run-target to load for the given profile')

    sub = subs.add_parser('init', parents=[base_subparser])
    sub.add_argument('project_name', type=str, help='Name of the new project')
    sub.set_defaults(cls=init_task.InitTask, which='init')

    sub = subs.add_parser('clean', parents=[base_subparser])
    sub.set_defaults(cls=clean_task.CleanTask, which='clean')

    sub = subs.add_parser('compile', parents=[base_subparser])
    sub.add_argument('--dry', action='store_true', help="Compile 'dry run' models")
    sub.set_defaults(cls=compile_task.CompileTask, which='compile')

    sub = subs.add_parser('debug', parents=[base_subparser])
    sub.set_defaults(cls=debug_task.DebugTask, which='debug')

    sub = subs.add_parser('deps', parents=[base_subparser])
    sub.set_defaults(cls=deps_task.DepsTask, which='deps')

    sub = subs.add_parser('run', parents=[base_subparser])
    sub.add_argument('--dry', action='store_true', help="'dry run' models")
    sub.add_argument('--models', required=False, nargs='+', help="Specify the models to run. All models depending on these models will also be run")
    sub.set_defaults(cls=run_task.RunTask, which='run')

    sub = subs.add_parser('seed', parents=[base_subparser])
    sub.add_argument('--drop-existing', action='store_true', help="Drop existing seed tables and recreate them")
    sub.set_defaults(cls=seed_task.SeedTask, which='seed')

    sub = subs.add_parser('test', parents=[base_subparser])
    sub.add_argument('--skip-test-creates', action='store_true', help="Don't create temporary views to validate model SQL")
    sub.add_argument('--validate', action='store_true', help='Run constraint validations from schema.yml files')
    sub.set_defaults(cls=test_task.TestTask, which='test')

    sub = subs.add_parser('hosted')
    hosted_sub = sub.add_subparsers(help='Hosted DBT commands')

    hosted_login = hosted_sub.add_parser('login')
    hosted_login.set_defaults(
        cls=hosted_auth_task.HostedAuthTask, which='hosted login')

    hosted_init = hosted_sub.add_parser('init')
    hosted_init.set_defaults(
        cls=hosted_initialize_task.HostedInitializeTask, which='hosted init')

    hosted_configure = hosted_sub.add_parser('configure')
    hosted_configure.set_defaults(
        cls=hosted_configure_task.HostedConfigureTask, which='hosted configure')

    hosted_push = hosted_sub.add_parser('push', parents=[base_subparser])
    hosted_push.set_defaults(
        cls=hosted_push_task.HostedPushTask, which='hosted push')

    if len(args) == 0: return p.print_help()

    parsed = p.parse_args(args)

    task = None
    proj = None

    if parsed.which == 'init' or parsed.which == 'hosted login' \
       or parsed.which == 'hosted init' or parsed.which == 'hosted configure':
        # bypass looking for a project file if we're running certain dbt cmds
        task = parsed.cls(args=parsed)

    elif os.path.isfile('dbt_project.yml'):
        try:
            proj = project.read_project('dbt_project.yml', validate=False).with_profiles(parsed.profile)
            proj.validate()
        except project.DbtProjectError as e:
            print("Encountered an error while reading the project:")
            print("  ERROR {}".format(str(e)))
            print("Did you set the correct --profile? Using: {}".format(parsed.profile))
            all_profiles = project.read_profiles().keys()
            profiles_string = "\n".join([" - " + key for key in all_profiles])
            print("Valid profiles:\n{}".format(profiles_string))
            dbt.tracking.track_invalid_invocation(project=proj, args=parsed, result_type="invalid_profile", result=str(e))
            return None

        if parsed.target is not None:
            targets = proj.cfg.get('outputs', {}).keys()
            if parsed.target in targets:
                proj.cfg['run-target'] = parsed.target
            else:
                print("Encountered an error while reading the project:")
                print("  ERROR Specified target {} is not a valid option for profile {}".format(parsed.target, parsed.profile))
                print("Valid targets are: {}".format(targets))
                dbt.tracking.track_invalid_invocation(project=proj, args=parsed, result_type="invalid_target", result="target not found")
                return None

        log_dir = proj.get('log-path', 'logs')
        logger = getLogger(log_dir, __name__)

        logger.info("running dbt with arguments %s", parsed)

        task = parsed.cls(args=parsed, project=proj)

    else:
        raise RuntimeError("dbt must be run from a project root directory with a dbt_project.yml file")

    dbt.tracking.track_invocation_start(project=proj, args=parsed)
    try:
        task.run()
        dbt.tracking.track_invocation_end(project=proj, args=parsed, result_type="ok", result=None)
    except Exception as e:
        dbt.tracking.track_invocation_end(project=proj, args=parsed, result_type="error", result=str(e))
        raise
