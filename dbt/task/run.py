from __future__ import print_function

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager

THREAD_LIMIT = 9


class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def run(self):
        runner = RunManager(
            self.project, self.project['target-path'], self.args
        )

        results = runner.run_models(self.args.models, self.args.exclude)

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
