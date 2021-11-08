import os
# import shutil
import hashlib
import tarfile
from tempfile import NamedTemporaryFile
from typing import Optional

from dbt.clients import system
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.config import Project
from dbt.contracts.project import (
    ProjectPackageMetadata,
    TarballPackage,
)
# from dbt.logger import GLOBAL_LOGGER as logger
from dbt.logger import GLOBAL_LOGGER as logger

TARFILE_MAX_SIZE = 1 * 1e+6  # limit tarfiles to 1mb


class TarballPackageMixin:
    def __init__(self, tarball: str) -> None:
        super().__init__()
        self.tarball = tarball

    @property
    def name(self):
        return self.tarball

    def source_type(self):
        return 'tarball'


class TarballPinnedPackage(TarballPackageMixin, PinnedPackage):
    def __init__(self, tarball: str) -> None:
        super().__init__(tarball)
        self.tarfile = self.get_tarfile()
        # init tarfile in class, and use tarfile as cache
        # simpler for tempfile handling, and prevent multiple hits to
        # url (one for metadata, one for intall)
        self.tar_dir_name = self.validate_tarfile()
        # assumed structure is that tarfile has a root dir (package name)
        # but we don't know what it is. will scan file for best candidate

    def get_version(self):
        return None

    def nice_version_name(self):
        return '<tarball @ {}>'.format(self.tarball)

    def get_tarfile(self):
        from tempfile import NamedTemporaryFile
        import tarfile
        def get_tar_size(TarFile: tarfile.TarFile):
            return sum(x.size for x in TarFile.getmembers())

        with NamedTemporaryFile(dir=get_downloads_path()) as named_temp_file:
            # NamedTemporaryFile on top of get_downloads_path
            # can mean NamedTemporaryFile in TemporaryFile, but works fine
            print('\n\nHAI - tempfile?')
            print(named_temp_file.name)

            download_url = self.tarball
            system.download_with_retries(download_url, named_temp_file.name)

            print('\n\nHAI - got a file now?')

            assert tarfile.is_tarfile(named_temp_file.name), "xxx not TAR!"
            print('\n\nHAI - file looks good')

            tar = tarfile.open(named_temp_file.name, "r:gz")

            msg = (f"{named_temp_file.name} size is larger than limit "
                   "of {TARFILE_MAX_SIZE}.")
            assert get_tar_size(tar) <= TARFILE_MAX_SIZE, msg
        return tar

    def validate_tarfile(self):
        ''' assumed structure is that tarfile has a root dir (package name)
        but we don't know what it is. will look for lone dir on root and
        use that, error if multiple dirs on root (better way?)
          todo? optional tarball root_dir arg (e.g. '.', 'mything', 'whatever) 
          in package file?'''
        tar_dir_name = system.resolve_tar_dir_name(self.tarfile)

        assert tar_dir_name != '', "package structure malformed"

        return tar_dir_name

    def _fetch_metadata(self, project, renderer):
        tarfile = self.tarfile
        tar_dir_name = self.tar_dir_name

        tar_path = os.path.realpath(
            os.path.join(get_downloads_path(), tar_dir_name)
        )
        system.make_directory(os.path.dirname(tar_path))

        tarfile.extractall(path=tar_path)

        tar_extracted_root = os.path.join(tar_path, tar_dir_name)

        loaded = Project.from_project_root(tar_extracted_root, renderer)

        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project, renderer):
        tar_dir_name = self.tar_dir_name

        tar_path = os.path.realpath(
            os.path.join(get_downloads_path(), tar_dir_name)
        )
        system.make_directory(os.path.dirname(tar_path))
        # tar_extracted_root = os.path.join(tar_path, tar_dir_name)

        # loaded = Project.from_project_root(tar_extracted_root, renderer)

        deps_path = project.packages_install_path
        package_name = self.get_project_name(project, renderer)
        # tarfile.extractall(path=deps_path)

        system.untar_tarfile(TarFile=self.tarfile, dest_dir=deps_path,
                             rename_to=package_name)


class TarballUnpinnedPackage(
    TarballPackageMixin, UnpinnedPackage[TarballPinnedPackage]
):
    @classmethod
    def from_contract(
        cls, contract: TarballPackage
    ) -> 'TarballPinnedPackage':
        return cls(tarball=contract.tarball)

    def incorporate(
        self, other: 'TarballUnpinnedPackage'
    ) -> 'TarballUnpinnedPackage':
        return TarballUnpinnedPackage(tarball=self.tarball)

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(tarball=self.tarball)
