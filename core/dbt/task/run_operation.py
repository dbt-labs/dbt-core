from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base_task import BaseTask
from dbt.adapters.factory import get_adapter
from dbt.loader import GraphLoader

import dbt
import dbt.utils
import dbt.exceptions


class RunOperationTask(BaseTask):
    def _get_macro_parts(self):
        macro_name = self.args.macro
        if '.' in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        return package_name, macro_name

    def _get_kwargs(self):
        return dbt.utils.parse_cli_vars(self.args.args)

    def run(self):
        manifest = GraphLoader.load_all(self.config)
        adapter = get_adapter(self.config)

        package_name, macro_name = self._get_macro_parts()
        macro_kwargs = self._get_kwargs()

        res = adapter.execute_macro(
            macro_name,
            project=package_name,
            kwargs=macro_kwargs,
            manifest=manifest,
            connection_name="macro_{}".format(macro_name)
        )

        return res
