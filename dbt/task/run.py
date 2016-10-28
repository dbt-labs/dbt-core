
from __future__ import print_function

import os
from dbt.templates import DryCreateTemplate, BaseCreateTemplate
from dbt.runner import RunManager
from dbt.compilation import Compiler, CompilableEntities

THREAD_LIMIT = 9

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def compile(self):
        create_template = DryCreateTemplate if self.args.dry else BaseCreateTemplate
        compiler = Compiler(self.project, create_template)
        compiler.initialize()
        results = compiler.compile(self.args.dry)

        stat_line = ", ".join(["{} {}".format(results[k], k) for k in CompilableEntities])
        print("Compiled {}".format(stat_line))

        return create_template.label

    def run(self):
        graph_type = self.compile()

        runner = RunManager(self.project, self.project['target-path'], graph_type, self.args.threads)

        if self.args.dry:
            results = runner.dry_run(self.args.models)
        else:
            results = runner.run(self.args.models)

        total   = len(results)
        passed  = len([r for r in results if not r.errored and not r.skipped])
        errored = len([r for r in results if r.errored])
        skipped = len([r for r in results if r.skipped])


        print()
        print("Done. PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}".format(total=total, passed=passed, errored=errored, skipped=skipped))
