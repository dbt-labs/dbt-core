
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.ui.printer
import dbt.adapters.factory

import sys


class AdapterTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def install(self, adapter_type):
        logger.info("Installing {}".format(adapter_type))
        dbt.adapters.factory.install_adapter(adapter_type)

    def list(self):
        logger.info("Using python {}".format(sys.executable))
        adapters = dbt.adapters.factory.list_adapters()
        logger.info("Adapters:")
        for name, is_installed in adapters.items():
            if is_installed:
                installed_str = dbt.ui.printer.green("INSTALLED")
            else:
                installed_str = dbt.ui.printer.yellow("NOT INSTALLED")
            formatted_name = name.rjust(10)
            logger.info(" {}: {}".format(formatted_name, installed_str))

    def run(self):
        if self.args.list:
            self.list()

        elif self.args.install:
            self.install(self.args.install)

        else:
            self.list()
