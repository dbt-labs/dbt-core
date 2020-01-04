from typing import Optional

import dbt.utils
import dbt.deprecations
import dbt.exceptions

from dbt.config import Project
from dbt.deps.base import downloads_directory
from dbt.deps.resolver import resolve_packages

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.clients import system

from dbt.task.base import ProjectOnlyTask


class DepsTask(ProjectOnlyTask):
    def __init__(self, args, config: Optional[Project] = None):
        super().__init__(args=args, config=config)

    def track_package_install(self, package_name, source_type, version):
        version = 'local' if source_type == 'local' else version

        h_package_name = dbt.utils.md5(package_name)
        h_version = dbt.utils.md5(version)

        dbt.tracking.track_package_install({
            "name": h_package_name,
            "source": source_type,
            "version": h_version
        })

    def run(self):
        system.make_directory(self.config.modules_path)
        packages = self.config.packages.packages
        if not packages:
            logger.info('Warning: No packages were found in packages.yml')
            return

        with downloads_directory():
            final_deps = resolve_packages(packages, self.config)

            for package in final_deps:
                logger.info('Installing {}', package)
                package.install(self.config)
                logger.info('  Installed from {}\n',
                            package.nice_version_name())

                self.track_package_install(
                    package_name=package.name,
                    source_type=package.source_type(),
                    version=package.get_version())
