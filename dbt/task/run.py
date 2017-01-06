from __future__ import print_function

import os

from dbt.compilation import Compiler, CompilableEntities
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.templates import DryCreateTemplate, BaseCreateTemplate

THREAD_LIMIT = 9


class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        create_template = DryCreateTemplate if self.args.dry \
                          else BaseCreateTemplate
        compiler = Compiler(self.project, create_template, self.args)
        compiler.initialize()
        results = compiler.compile(limit_to=['models'])

        stat_line = ", ".join([
            "{} {}".format(results[k], k) for k in CompilableEntities
        ])
        logger.info("Compiled {}".format(stat_line))

        return create_template.label

    def run(self):
        graph_type = self.compile()

        runner = RunManager(
            self.project, self.project['target-path'], graph_type, self.args
        )

        if self.args.dry:
            results = runner.dry_run(self.args.models)
        else:
            results = runner.run(self.args.models)

        total = len(results)
        passed = len([r for r in results if not r.errored and not r.skipped])
        errored = len([r for r in results if r.errored])
        skipped = len([r for r in results if r.skipped])

        logger.info(
            "Done. PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}"
            .format(
                total=total,
                passed=passed,
                errored=errored,
                skipped=skipped
            )
        )
