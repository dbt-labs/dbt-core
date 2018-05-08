
from pprint import pformat

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.factory import get_adapter
import dbt.ui.printer

from dbt.task.base_task import BaseTask


# derive from BaseTask as I don't really want any result interpretation.
class GenerateTask(BaseTask):
    def run(self):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        # To get a list of schemas, it looks like we'd need to have the
        # compiled project and use node_runners.BaseRunner.get_model_schemas.
        # But I think we don't really want to compile, here, right? Or maybe
        # we do and I need to add all of that? But then we probably need to
        # go through the whole BaseRunner.safe_run path which makes things
        # more complex - need to figure out how to handle all the
        # is_ephemeral_model stuff, etc.
        # TODO: talk to connor/drew about this question.
        try:
            results = adapter.get_catalog_for_schemas(profile, schemas=None)
        finally:
            adapter.cleanup_connections()

        # dump results to stdout for the moment.
        dbt.ui.printer.print_timestamped_line('results={}'.format(pformat(results)))
        dbt.ui.printer.print_timestamped_line('Done.')

        return results
