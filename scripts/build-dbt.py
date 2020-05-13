import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import venv  # type: ignore
import zipfile

from typing import Dict

from argparse import ArgumentParser
from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

from typing import Optional, Iterator, Tuple, List


HOMEBREW_PYTHON = (3, 8)


# This should match the pattern in .bumpversion.cfg
VERSION_PATTERN = re.compile(
    r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)'
    r'((?P<prerelease>[a-z]+)(?P<num>\d+))?'
)


class Version:
    def __init__(self, raw: str) -> None:
        self.raw = raw
        match = VERSION_PATTERN.match(self.raw)
        assert match is not None, f'Invalid version: {self.raw}'
        groups = match.groupdict()

        self.major: int = int(groups['major'])
        self.minor: int = int(groups['minor'])
        self.patch: int = int(groups['patch'])
        self.prerelease: Optional[str] = None
        self.num: Optional[int] = None

        if groups['num'] is not None:
            self.prerelease = groups['prerelease']
            self.num = int(groups['num'])

    def __str__(self):
        return self.raw

    def homebrew_class_name(self) -> str:
        name = f'DbtAT{self.major}{self.minor}{self.patch}'
        if self.prerelease is not None and self.num is not None:
            name = f'{name}{self.prerelease.title()}{self.num}'
        return name

    def homebrew_filename(self):
        version_str = f'{self.major}.{self.minor}.{self.patch}'
        if self.prerelease is not None and self.num is not None:
            version_str = f'{version_str}-{self.prerelease}{self.num}'
        return f'dbt@{version_str}.rb'


@dataclass
class Arguments:
    version: Version
    part: str
    path: Path
    homebrew_path: Path
    homebrew_set_default: bool
    set_version: bool
    build_pypi: bool
    upload_pypi: bool
    test_upload: bool
    build_homebrew: bool
    build_docker: bool
    upload_docker: bool
    write_requirements: bool
    write_dockerfile: bool

    @classmethod
    def parse(cls) -> 'Arguments':
        parser = ArgumentParser(
            prog="Bump dbt's version, build packages"
        )
        parser.add_argument(
            'version',
            type=Version,
            help="The version to set",
        )
        parser.add_argument(
            'part',
            type=str,
            help="The part of the version to update",
        )
        parser.add_argument(
            '--path',
            type=Path,
            help='The path to the dbt repository',
            default=Path.cwd(),
        )
        parser.add_argument(
            '--homebrew-path',
            type=Path,
            help='The path to the dbt homebrew install',
            default=(Path.cwd() / '../homebrew-dbt'),
        )
        parser.add_argument(
            '--homebrew-set-default',
            action='store_true',
            help='If set, make this homebrew version the default',
        )
        parser.add_argument(
            '--no-set-version',
            dest='set_version',
            action='store_false',
            help='Skip bumping the version',
        )
        parser.add_argument(
            '--no-build-pypi',
            dest='build_pypi',
            action='store_false',
            help='skip building pypi',
        )
        parser.add_argument(
            '--no-build-docker',
            dest='build_docker',
            action='store_false',
            help='skip building docker images',
        )
        parser.add_argument(
            '--no-upload-docker',
            dest='upload_docker',
            action='store_false',
            help='skip uploading docker images',
        )

        uploading = parser.add_mutually_exclusive_group()

        uploading.add_argument(
            '--upload-pypi',
            dest='force_upload_pypi',
            action='store_true',
            help='upload to pypi even if building is disabled'
        )

        uploading.add_argument(
            '--no-upload-pypi',
            dest='no_upload_pypi',
            action='store_true',
            help='skip uploading to pypi',
        )

        parser.add_argument(
            '--no-upload',
            dest='test_upload',
            action='store_false',
            help='Skip uploading to pypitest',
        )

        parser.add_argument(
            '--no-build-homebrew',
            dest='build_homebrew',
            action='store_false',
            help='Skip building homebrew packages',
        )
        parser.add_argument(
            '--no-write-requirements',
            dest='write_requirements',
            action='store_false',
            help='Skip writing the requirements file. It must exist.'
        )
        parser.add_argument(
            '--no-write-dockerfile',
            dest='write_dockerfile',
            action='store_false',
            help='Skip writing the dockerfile. It must exist.'
        )
        parsed = parser.parse_args()

        upload_pypi = parsed.build_pypi
        if parsed.force_upload_pypi:
            upload_pypi = True
        elif parsed.no_upload_pypi:
            upload_pypi = False

        return cls(
            version=parsed.version,
            part=parsed.part,
            path=parsed.path,
            homebrew_path=parsed.homebrew_path,
            homebrew_set_default=parsed.homebrew_set_default,
            set_version=parsed.set_version,
            build_pypi=parsed.build_pypi,
            upload_pypi=upload_pypi,
            test_upload=parsed.test_upload,
            build_homebrew=parsed.build_homebrew,
            build_docker=parsed.build_docker,
            upload_docker=parsed.upload_docker,
            write_requirements=parsed.write_requirements,
            write_dockerfile=parsed.write_dockerfile,
        )


def collect_output(cmd, cwd=None, stderr=subprocess.PIPE) -> str:
    try:
        result = subprocess.run(
            cmd, cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=stderr
        )
    except subprocess.CalledProcessError as exc:
        print(f'Command {exc.cmd} failed')
        if exc.output:
            print(exc.output.decode('utf-8'))
        if exc.stderr:
            print(exc.stderr.decode('utf-8'), file=sys.stderr)
        raise
    return result.stdout.decode('utf-8')


def run_command(cmd, cwd=None) -> None:
    result = collect_output(cmd, stderr=subprocess.STDOUT, cwd=cwd)
    print(result)


def set_version(path: Path, version: Version, part: str):
    # bumpversion --commit --no-tag --new-version "${version}" "${port}"
    cmd = [
        'bumpversion', '--commit', '--no-tag', '--new-version',
        str(version), part
    ]
    print(f'bumping version to {version}')
    run_command(cmd, cwd=path)
    print(f'bumped version to {version}')


class PypiBuilder:
    _SUBPACKAGES = (
        'core',
        'plugins/postgres',
        'plugins/redshift',
        'plugins/bigquery',
        'plugins/snowflake',
    )

    def __init__(self, dbt_path: Path):
        self.dbt_path = dbt_path

    @staticmethod
    def _dist_for(path: Path, make=False) -> Path:
        dist_path = path / 'dist'
        if dist_path.exists():
            shutil.rmtree(dist_path)
        if make:
            os.makedirs(dist_path)
        build_path = path / 'build'
        if build_path.exists():
            shutil.rmtree(build_path)
        return dist_path

    @staticmethod
    def _build_pypi_package(path: Path):
        print(f'building package in {path}')
        cmd = ['python', 'setup.py', 'sdist', 'bdist_wheel']
        run_command(cmd, cwd=path)
        print(f'finished building package in {path}')

    @staticmethod
    def _all_packages_in(path: Path) -> Iterator[Path]:
        path = path / 'dist'
        for pattern in ('*.tar.gz', '*.whl'):
            yield from path.glob(pattern)

    def _build_subpackage(self, name: str) -> Iterator[Path]:
        subpath = self.dbt_path / name
        self._dist_for(subpath)
        self._build_pypi_package(subpath)
        return self._all_packages_in(subpath)

    def build(self):
        print('building pypi packages')
        dist_path = self._dist_for(self.dbt_path)
        sub_pkgs: List[Path] = []
        for path in self._SUBPACKAGES:
            sub_pkgs.extend(self._build_subpackage(path))

        # now build the main package
        self._build_pypi_package(self.dbt_path)
        # now copy everything from the subpackages in
        for package in sub_pkgs:
            shutil.copy(str(package), dist_path)

        print('built pypi packages')

    def upload(self, *, test=True):
        cmd = ['twine', 'check']
        cmd.extend(str(p) for p in self._all_packages_in(self.dbt_path))
        run_command(cmd)
        cmd = ['twine', 'upload']
        if test:
            cmd.extend(['--repository', 'pypitest'])
        cmd.extend(str(p) for p in self._all_packages_in(self.dbt_path))
        print('uploading packages: {}'.format(' '.join(cmd)))
        run_command(cmd)
        print('uploaded packages')


class PipInstaller(venv.EnvBuilder):
    def __init__(self, packages: List[str]) -> None:
        super().__init__(with_pip=True)
        self.packages = packages

    def post_setup(self, context):
        # we can't run from the dbt directory or this gets all weird, so
        # install from an empty temp directory and then remove it.
        tmp = tempfile.mkdtemp()
        cmd = [context.env_exe, '-m', 'pip', 'install', '--upgrade']
        cmd.extend(self.packages)
        print(f'installing {self.packages}')
        try:
            run_command(cmd, cwd=tmp)
        finally:
            os.rmdir(tmp)
        print(f'finished installing {self.packages}')

    def create(self, venv_path):
        os.makedirs(venv_path.parent, exist_ok=True)
        if venv_path.exists():
            shutil.rmtree(venv_path)
        return super().create(venv_path)


def _require_wheels(dbt_path: Path) -> List[Path]:
    dist_path = dbt_path / 'dist'
    wheels = list(dist_path.glob('*.whl'))
    if not wheels:
        raise ValueError(
            f'No wheels found in {dist_path} - run scripts/build-wheels.sh'
        )
    return wheels


class DistFolderEnv(PipInstaller):
    def __init__(self, dbt_path: Path) -> None:
        self.wheels = _require_wheels(dbt_path)
        super().__init__(packages=self.wheels)


class PoetVirtualenv(PipInstaller):
    def __init__(self, dbt_version: Version) -> None:
        super().__init__([f'dbt=={dbt_version}', 'homebrew-pypi-poet'])


@dataclass
class HomebrewTemplate:
    url_data: str
    hash_data: str
    dependencies: str


def _make_venv_at(root: Path, name: str, builder: venv.EnvBuilder):
    venv_path = root / name
    os.makedirs(root, exist_ok=True)
    if venv_path.exists():
        shutil.rmtree(venv_path)

    builder.create(venv_path)
    return venv_path


class HomebrewBuilder:
    def __init__(
        self,
        dbt_path: Path,
        version: Version,
        homebrew_path: Path,
        set_default: bool,
    ) -> None:
        self.dbt_path = dbt_path
        self.version = version
        self.homebrew_path = homebrew_path
        self.set_default = set_default
        self._template: Optional[HomebrewTemplate] = None

    def make_venv(self) -> PoetVirtualenv:
        env = PoetVirtualenv(self.version)
        max_attempts = 10
        for attempt in range(1, max_attempts+1):
            # after uploading to pypi, it can take a few minutes for installing
            # to work. Retry a few times...
            try:
                env.create(self.homebrew_venv_path)
                return
            except subprocess.CalledProcessError:
                if attempt == max_attempts:
                    raise
                else:
                    print(
                        f'installation failed - waiting 60s for pypi to see '
                        f'the new version (attempt {attempt}/{max_attempts})'
                    )
                    time.sleep(60)

        return env

    @property
    def versioned_formula_path(self) -> Path:
        return (
            self.homebrew_path / 'Formula' / self.version.homebrew_filename()
        )

    @property
    def default_formula_path(self) -> Path:
        return (
            self.homebrew_path / 'Formula/dbt.rb'
        )

    @property
    def homebrew_venv_path(self) -> Path:
        return self.dbt_path / 'build' / 'homebrew-venv'

    @staticmethod
    def _dbt_homebrew_formula_fmt() -> str:
        return textwrap.dedent('''\
            class {formula_name} < Formula
              include Language::Python::Virtualenv

              desc "Data build tool"
              homepage "https://github.com/fishtown-analytics/dbt"
              url "{url_data}"
              sha256 "{hash_data}"
              revision 1

              bottle do
                root_url "http://bottles.getdbt.com"
                # bottle hashes + versions go here
              end

              depends_on "openssl@1.1"
              depends_on "postgresql"
              depends_on "python"

            {dependencies}
            {trailer}
            end
            ''')

    @staticmethod
    def _dbt_homebrew_trailer() -> str:
        dedented = textwrap.dedent('''\
          def install
            venv = virtualenv_create(libexec, "python3")

            res = resources.map(&:name).to_set

            res.each do |r|
              venv.pip_install resource(r)
            end

            venv.pip_install_and_link buildpath

            bin.install_symlink "#{libexec}/bin/dbt" => "dbt"
          end

          test do
            (testpath/"dbt_project.yml").write(
              "{name: 'test', version: '0.0.1', profile: 'default'}",
            )
            (testpath/".dbt/profiles.yml").write(
              "{default: {outputs: {default: {type: 'postgres', threads: 1,
              host: 'localhost', port: 5432, user: 'root', pass: 'password',
              dbname: 'test', schema: 'test'}}, target: 'default'}}",
            )
            (testpath/"models/test.sql").write("select * from test")
            system "#{bin}/dbt", "test"
          end''')
        return textwrap.indent(dedented, '  ')

    def get_formula_data(
        self, versioned: bool = True
    ) -> str:
        fmt = self._dbt_homebrew_formula_fmt()
        trailer = self._dbt_homebrew_trailer()
        if versioned:
            formula_name = self.version.homebrew_class_name()
        else:
            formula_name = 'Dbt'

        return fmt.format(
            formula_name=formula_name,
            version=self.version,
            url_data=self.template.url_data,
            hash_data=self.template.hash_data,
            dependencies=self.template.dependencies,
            trailer=trailer,
        )

    @property
    def template(self) -> HomebrewTemplate:
        if self._template is None:
            self.make_venv()
            print('done setting up virtualenv')
            poet = self.homebrew_venv_path / 'bin/poet'

            # get the dbt package info
            url_data, hash_data = self._get_pypi_dbt_info()

            dependencies = self._get_recursive_dependencies(poet)
            template = HomebrewTemplate(
                url_data=url_data,
                hash_data=hash_data,
                dependencies=dependencies,
            )
            self._template = template
        else:
            template = self._template
        return template

    def _get_pypi_dbt_info(self) -> Tuple[str, str]:
        fp = urlopen(f'https://pypi.org/pypi/dbt/{self.version}/json')
        try:
            data = json.load(fp)
        finally:
            fp.close()
        assert 'urls' in data
        for pkginfo in data['urls']:
            assert 'packagetype' in pkginfo
            if pkginfo['packagetype'] == 'sdist':
                assert 'url' in pkginfo
                assert 'digests' in pkginfo
                assert 'sha256' in pkginfo['digests']
                url = pkginfo['url']
                digest = pkginfo['digests']['sha256']
                return url, digest
        raise ValueError(f'Never got a valid sdist for dbt=={self.version}')

    def _get_recursive_dependencies(self, poet_exe: Path) -> str:
        cmd = [str(poet_exe), '--resources', 'dbt']
        raw = collect_output(cmd).split('\n')
        return '\n'.join(self._remove_dbt_resource(raw))

    def _remove_dbt_resource(self, lines: List[str]) -> Iterator[str]:
        # TODO: fork poet or extract the good bits to avoid this
        line_iter = iter(lines)
        # don't do a double-newline or "brew audit" gets mad
        for line in line_iter:
            # skip the contents of the "dbt" resource block.
            if line.strip() == 'resource "dbt" do':
                for skip in line_iter:
                    if skip.strip() == 'end':
                        # skip the newline after 'end'
                        next(line_iter)
                        break
            else:
                yield line

    def create_versioned_formula_file(self):
        formula_contents = self.get_formula_data(versioned=True)
        if self.versioned_formula_path.exists():
            print('Homebrew formula path already exists, overwriting')
        self.versioned_formula_path.write_text(formula_contents)

    def commit_versioned_formula(self):
        # add a commit for the new formula
        run_command(
            ['git', 'add', self.versioned_formula_path],
            cwd=self.homebrew_path
        )
        run_command(
            ['git', 'commit', '-m', f'add dbt@{self.version}'],
            cwd=self.homebrew_path
        )

    def commit_default_formula(self):
        run_command(
            ['git', 'add', self.default_formula_path],
            cwd=self.homebrew_path
        )
        run_command(
            ['git', 'commit', '-m', f'upgrade dbt to {self.version}'],
            cwd=self.homebrew_path
        )

    @staticmethod
    def run_tests(formula_path: Path, audit: bool = True):
        path = os.path.normpath(formula_path)
        run_command(['brew', 'uninstall', '--force', path])
        versions = [
            l.strip() for l in
            collect_output(['brew', 'list']).split('\n')
            if l.strip().startswith('dbt@') or l.strip() == 'dbt'
        ]
        if versions:
            run_command(['brew', 'unlink'] + versions)
        run_command(['brew', 'install', path])
        run_command(['brew', 'test', path])
        if audit:
            run_command(['brew', 'audit', '--strict', path])

    def create_default_package(self):
        os.remove(self.default_formula_path)
        formula_contents = self.get_formula_data(versioned=False)
        self.default_formula_path.write_text(formula_contents)

    def build(self):
        self.create_versioned_formula_file()
        # self.run_tests(formula_path=self.versioned_formula_path)
        self.commit_versioned_formula()

        if self.set_default:
            self.create_default_package()
            # self.run_tests(formula_path=self.default_formula_path, audit=False)
            self.commit_default_formula()


class WheelInfo:
    def __init__(self, path):
        self.path = path

    @staticmethod
    def _extract_distinfo_path(wfile: zipfile.ZipFile) -> zipfile.Path:
        zpath = zipfile.Path(root=wfile)
        for path in zpath.iterdir():
            if path.name.endswith('.dist-info'):
                return path
        raise ValueError('Wheel with no dist-info?')

    def get_metadata(self) -> Dict[str, str]:
        with zipfile.ZipFile(self.path) as wf:
            distinfo = self._extract_distinfo_path(wf)
            metadata = distinfo / 'METADATA'
            metadata_dict: Dict[str, str] = {}
            for line in metadata.read_text().split('\n'):
                parts = line.split(': ', 1)
                if len(parts) == 2:
                    metadata_dict[parts[0]] = parts[1]
            return metadata_dict

    def package_name(self) -> str:
        metadata = self.get_metadata()
        if 'Name' not in metadata:
            raise ValueError('Wheel with no name?')
        return metadata['Name']


class DockerBuilder:
    """The docker builder requires the existence of a dbt package"""
    def __init__(self, dbt_path: Path, version: Version) -> None:
        self.dbt_path = dbt_path
        self.version = version

    @property
    def docker_path(self) -> Path:
        return self.dbt_path / 'docker'

    @property
    def dockerfile_name(self) -> str:
        return f'Dockerfile.{self.version}'

    @property
    def dockerfile_path(self) -> Path:
        return self.docker_path / self.dockerfile_name

    @property
    def requirements_path(self) -> Path:
        return self.docker_path / 'requirements'

    @property
    def requirements_file_name(self) -> str:
        return f'requirements.{self.version}.txt'

    @property
    def dockerfile_venv_path(self) -> Path:
        return self.dbt_path / 'build' / 'docker-venv'

    @property
    def requirements_txt_path(self) -> Path:
        return self.requirements_path / self.requirements_file_name

    def make_venv(self) -> DistFolderEnv:
        env = DistFolderEnv(self.dbt_path)

        env.create(self.dockerfile_venv_path)
        return env

    def get_frozen(self) -> str:
        env = self.make_venv()
        pip_path = self.dockerfile_venv_path / 'bin/pip'
        cmd = [pip_path, 'freeze']
        wheel_names = {
            WheelInfo(wheel_path).package_name() for wheel_path in env.wheels
        }
        # remove the dependencies in dbt itself
        return '\n'.join([
            dep for dep in collect_output(cmd).split('\n')
            if dep.split('==')[0] not in wheel_names
        ])

    def write_lockfile(self):
        freeze = self.get_frozen()
        path = self.requirements_txt_path
        if path.exists():
            raise ValueError(f'Found existing requirements file at {path}!')
        os.makedirs(path.parent, exist_ok=True)
        path.write_text(freeze)

    def get_dockerfile_contents(self):
        dist_path = (self.dbt_path / 'dist').relative_to(Path.cwd())
        wheel_paths = ' '.join(
            os.path.join('.', 'dist', p.name)
            for p in _require_wheels(self.dbt_path)
        )

        requirements_path = self.requirements_txt_path.relative_to(Path.cwd())

        return textwrap.dedent(
            f'''\
            FROM python:3.8.1-slim-buster

            RUN apt-get update && \
                apt-get dist-upgrade -y && \
                apt-get install -y  --no-install-recommends \
                    git software-properties-common make build-essential \
                    ca-certificates libpq-dev && \
                apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

            COPY {requirements_path} ./{self.requirements_file_name}
            COPY {dist_path} ./dist
            RUN pip install --upgrade pip setuptools
            RUN pip install --requirement ./{self.requirements_file_name}
            RUN pip install {wheel_paths}

            RUN useradd -mU dbt_user

            ENV PYTHONIOENCODING=utf-8
            ENV LANG C.UTF-8

            WORKDIR /usr/app
            VOLUME /usr/app

            USER dbt_user
            CMD ['dbt', 'run']
            '''
        )

    def write_dockerfile(self):
        dockerfile = self.get_dockerfile_contents()
        path = self.dockerfile_path
        if path.exists():
            raise ValueError(f'Found existing docker file at {path}!')
        os.makedirs(path.parent, exist_ok=True)
        path.write_text(dockerfile)

    @property
    def image_tag(self):
        return f'dbt:{self.version}'

    @property
    def remote_tag(self):
        return f'fishtownanalytics/{self.image_tag}'

    def create_docker_image(self):
        run_command(
            [
                'docker', 'build',
                '-f', self.dockerfile_path,
                '--tag', self.image_tag,
                # '--no-cache',
                self.dbt_path,
            ],
            cwd=self.dbt_path
        )

    def set_remote_tag(self):
        # tag it
        run_command(
            ['docker', 'tag', self.image_tag, self.remote_tag],
            cwd=self.dbt_path,
        )

    def commit_docker_folder(self):
        # commit the contents of docker/
        run_command(
            ['git', 'add', 'docker'],
            cwd=self.dbt_path
        )
        commit_msg = f'Add {self.image_tag} dockerfiles and requirements'
        run_command(['git', 'commit', '-m', commit_msg], cwd=self.dbt_path)

    def build(
        self,
        write_requirements: bool = True,
        write_dockerfile: bool = True
    ):
        if write_requirements:
            self.write_lockfile()
        if write_dockerfile:
            self.write_dockerfile()
        self.commit_docker_folder()
        self.create_docker_image()
        self.set_remote_tag()

    def push(self):
        run_command(
            ['docker', 'push', self.remote_tag]
        )


def sanity_check():
    if sys.version_info[:len(HOMEBREW_PYTHON)] != HOMEBREW_PYTHON:
        python_version_str = '.'.join(str(i) for i in HOMEBREW_PYTHON)
        print(f'This script must be run with python {python_version_str}')
        sys.exit(1)

    # avoid "what's a bdist_wheel" errors
    try:
        import wheel  # type: ignore # noqa
    except ImportError:
        print(
            'The wheel package is required to build. Please run:\n'
            'pip install -r dev_requirements.txt'
        )
        sys.exit(1)


def upgrade_to(args: Arguments):
    if args.set_version:
        set_version(args.path, args.version, args.part)

    builder = PypiBuilder(args.path)
    if args.build_pypi:
        builder.build()

    if args.upload_pypi:
        if args.test_upload:
            builder.upload()
            input(
                f'Ensure https://test.pypi.org/project/dbt/{args.version}/ '
                'exists and looks reasonable'
            )
        builder.upload(test=False)

    if args.build_homebrew:
        if args.upload_pypi:
            print('waiting a minute for pypi before trying to pip install')
            # if we uploaded to pypi, wait a minute before we bother trying to
            # pip install
            time.sleep(60)
        HomebrewBuilder(
            dbt_path=args.path,
            version=args.version,
            homebrew_path=args.homebrew_path,
            set_default=args.homebrew_set_default,
        ).build()

    if args.build_docker:
        builder = DockerBuilder(
            dbt_path=args.path,
            version=args.version,
        )
        builder.build(
            write_requirements=args.write_requirements,
            write_dockerfile=args.write_dockerfile,
        )
        if args.upload_docker:
            builder.push()


def main():
    sanity_check()
    args = Arguments.parse()
    upgrade_to(args)


if __name__ == '__main__':
    main()
