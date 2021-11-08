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

from dbt.logger import GLOBAL_LOGGER as logger

TARFILE_MAX_SIZE = 1 * 1e+6  # limit tarfiles to 1mb


class TarballPackageMixin:
    def __init__(self, tarball: str) -> None:
        super().__init__()
        self.tarball = tarball

    @property
    def name(self):
        return self.tarball

    def source_type(self) -> str:
        return 'tarball'


class TarballPinnedPackage(TarballPackageMixin, PinnedPackage):
    def __init__(
        self, 
        tarball: str,
        sha1: Optional[str] = None,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        self.sha1 = sha1
        self.subdirectory = subdirectory
        self.tarfile = self.get_tarfile()
        # init tarfile in class, and use tarfile as cache
        # simpler for tempfile handling, and prevent multiple hits to
        # url (one for metadata, one for intall)
        self.tar_dir_name = self.resolve_tar_dir()
        # assumed structure is that tarfile has a root dir (package name)
        # but we don't know what it is. will scan file for best candidate
        # todo implement subdirectory like in git package

    def get_version(self):
        return None

    def nice_version_name(self):
        return '<tarball @ {}>'.format(self.tarball)

    def get_tarfile(self):
        def get_tar_size(TarFile: tarfile.TarFile):
            return sum(x.size for x in TarFile.getmembers())

        def file_sha1(fp: str):
            with open(named_temp_file.name, 'rb') as fp:
                return hashlib.sha1(fp.read()).hexdigest()

        with NamedTemporaryFile(dir=get_downloads_path()) as named_temp_file:
            # NamedTemporaryFile on top of get_downloads_path
            # can mean NamedTemporaryFile in TemporaryFile, but works fine
            logger.debug(f"Using NamedTemporaryFile {named_temp_file.name}")
            download_url = self.tarball
            system.download_with_retries(download_url, named_temp_file.name)

            msg = f"{download_url} is not a valid tarfile."
            assert tarfile.is_tarfile(named_temp_file.name), msg

            tar = tarfile.open(named_temp_file.name, "r:gz")

            msg = (f"{named_temp_file.name} size is larger than limit "
                   "of {TARFILE_MAX_SIZE}.")
            assert get_tar_size(tar) <= TARFILE_MAX_SIZE, msg

            if self.sha1:
                checksum = file_sha1(named_temp_file.name)
                msg = ("sha1 mismatch for downloaded file. "
                       f"Actual: [{checksum}], expected: [{self.sha1}]. ")
                assert checksum == self.sha1, msg
                logger.debug(f"sha1 checksum passes ({self.sha1})")

        return tar

    def resolve_tar_dir(self):
        ''' assumed structure is that tarfile has a root dir (package name)
        but we don't know what it is. will look for lone dir on root and
        use that.
        optional - use subdirectory arg to manually specify, like used in git 
        package'''
        if not self.subdirectory:
            tar_dir_name = system.resolve_tar_dir_name(self.tarfile)
            debug_txt = 'resolved '
            msg = ("Package structure malformed. Expected one parent folder in"
                   " tar root. Try using the subdirectory setting to specify"
                   " path to package dir, or rebuilding the package "
                   "structure.")
            assert tar_dir_name != '', msg
        else:
            tar_dir_name = self.subdirectory
            debug_txt = 'specified '

        logger.debug(f"Using {debug_txt} {tar_dir_name}/ directory as "
                     "project root in tarfile.")

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
    def __init__(
        self, 
        tarball: str,
        sha1: Optional[str] = None,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(tarball)
        self.sha1 = sha1
        self.subdirectory = subdirectory

    @classmethod
    def from_contract(
        cls, contract: TarballPackage
    ) -> 'TarballUnpinnedPackage':
        return cls(
            tarball=contract.tarball, sha1=contract.sha1,
            subdirectory=contract.subdirectory
        )

    def incorporate(
        self, other: 'TarballUnpinnedPackage'
    ) -> 'TarballUnpinnedPackage':
        return TarballUnpinnedPackage(
            tarball=self.tarball, sha1=self.sha1,
            subdirectory=self.subdirectory
        )

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(
            tarball=self.tarball, sha1=self.sha1,
            subdirectory=self.subdirectory
        )
