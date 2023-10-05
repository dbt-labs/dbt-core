from pathlib import Path
from shutil import rmtree

from dbt import deprecations
from dbt.events.functions import fire_event
from dbt.events.types import (
    CheckCleanPath,
    ConfirmCleanPath,
    FinishedCleanPaths,
)
from dbt.exceptions import DbtRuntimeError
from dbt.task.base import (
    BaseTask,
    move_to_nearest_project_dir,
)


class CleanTask(BaseTask):
    def run(self):
        """
        This function takes all the paths in the target file
        and cleans the project paths that are not protected.
        """
        project_dir = move_to_nearest_project_dir(self.args.project_dir)

        potential_clean_paths = set(Path(p).resolve() for p in self.project.clean_targets)
        source_paths = set(
            Path(p).resolve() for p in (*self.project.all_source_paths, *self.project.test_paths)
        )
        clean_paths = potential_clean_paths.difference(source_paths)

        if potential_clean_paths != clean_paths:
            raise DbtRuntimeError(
                f"dbt will not clean the following source paths: {[str(s) for s in source_paths.intersection(potential_clean_paths)]}"
            )

        paths_outside_project = set(
            path for path in clean_paths if project_dir not in path.absolute().parents
        )
        if paths_outside_project and self.args.clean_project_files_only:
            raise DbtRuntimeError(
                f"dbt will not clean the following directories outside the project: {[str(p) for p in paths_outside_project]}"
            )

        if (
            "dbt_modules" in self.project.clean_targets
            and self.config.packages_install_path not in self.config.clean_targets
        ):
            deprecations.warn("install-packages-path")

        for path in clean_paths:
            fire_event(CheckCleanPath(path=str(path)))
            rmtree(path, True)
            fire_event(ConfirmCleanPath(path=str(path)))

        fire_event(FinishedCleanPaths())
