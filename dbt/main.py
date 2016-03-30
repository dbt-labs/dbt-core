import argparse
import os.path
import dbt.project as project
import dbt.task.run as run_task
import dbt.task.compile as compile_task
import dbt.task.debug as debug_task
import dbt.task.init as init_task
import dbt.task.clean as clean_task


def main(args):
    if os.path.isfile('dbt_project.yml'):
        proj = project.read_project('dbt_project.yml')
    else:
        proj = project.default_project()

    p = argparse.ArgumentParser(prog='dbt: data build tool')
    subs = p.add_subparsers()

    sub = subs.add_parser('clean')
    sub.set_defaults(cls=clean_task.CleanTask)

    sub = subs.add_parser('run')
    sub.set_defaults(cls=run_task.RunTask)

    sub = subs.add_parser('compile')
    sub.set_defaults(cls=compile_task.CompileTask)

    sub = subs.add_parser('init')
    sub.set_defaults(cls=init_task.InitTask)

    sub = subs.add_parser('debug')
    sub.set_defaults(cls=debug_task.DebugTask)

    parsed = p.parse_args(args)

    parsed.cls(args=parsed, project=proj).run()
