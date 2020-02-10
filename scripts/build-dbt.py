import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import venv  # type: ignore

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
            help='Skip uploading to pypi',
        )

        parser.add_argument(
            '--no-build-homebrew',
            dest='build_homebrew',
            action='store_false',
            help='Skip building homebrew packages'

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
        cmd = ['twine', 'upload']
        if test:
            cmd.extend(['--repository', 'pypitest'])
        cmd.extend(str(p) for p in self._all_packages_in(self.dbt_path))
        print('uploading packages: {}'.format(' '.join(cmd)))
        run_command(cmd)
        print('uploaded packages')


class PoetVirtualenv(venv.EnvBuilder):
    def __init__(self, dbt_version: Version) -> None:
        super().__init__(with_pip=True)
        self.dbt_version = dbt_version

    def post_setup(self, context):
        # we can't run from the dbt directory or this gets all weird, so
        # install from an empty temp directory and then remove it.
        tmp = tempfile.mkdtemp()
        cmd = [
            context.env_exe, '-m', 'pip', 'install', '--upgrade',
            'homebrew-pypi-poet', f'dbt=={self.dbt_version}'
        ]
        print(f'installing homebrew-pypi-poet and dbt=={self.dbt_version}')
        try:
            run_command(cmd, cwd=tmp)
        finally:
            os.rmdir(tmp)
        print(f'finished installing homebrew-pypi-poet and dbt')


@dataclass
class HomebrewTemplate:
    url_data: str
    hash_data: str
    dependencies: str


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

    def make_venv(self) -> Path:
        build_path = self.dbt_path / 'build'
        venv_path = build_path / 'tmp-venv'
        os.makedirs(build_path, exist_ok=True)
        if venv_path.exists():
            shutil.rmtree(venv_path)

        env = PoetVirtualenv(self.version)
        env.create(venv_path)
        return venv_path

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
            env_path = self.make_venv()
            print('done setting up virtualenv')
            poet = env_path / 'bin/poet'

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

    @staticmethod
    def _get_recursive_dependencies(poet_exe: Path) -> str:
        cmd = [str(poet_exe), '--resources', 'dbt']
        raw = collect_output(cmd)
        lines = []
        skipping = False
        # don't do a double-newline or "brew audit" gets mad
        skip_next = False
        for line in raw.split('\n'):
            # TODO: fork poet or extract the good bits to avoid this
            if skipping:
                if line.strip() == 'end':
                    skipping = False
                    skip_next = True
            elif skip_next is True:
                skip_next = False
            else:
                if line.strip() == 'resource "dbt" do':
                    skipping = True
                else:
                    lines.append(line)
        return '\n'.join(lines)

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
    def run_tests(formula_path: Path):
        path = os.path.normpath(formula_path)
        run_command(['brew', 'uninstall', '--force', path])
        run_command(['brew', 'install', path])
        run_command(['brew', 'test', path])
        run_command(['brew', 'audit', '--strict', path])

    def create_default_package(self):
        os.remove(self.default_formula_path)
        formula_contents = self.create_formula_data(versioned=False)
        self.default_formula_path.write_text(formula_contents)

    def build(self):
        self.create_versioned_formula_file()
        self.run_tests(formula_path=self.versioned_formula_path)
        self.commit_versioned_formula()

        if self.set_default:
            self.create_default_package()
            self.run_tests(formula_path=self.default_formula_path)
            self.commit_default_formula()


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
            'pip install -r dev_requirements'
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
                f'Ensure https://test.pypi.org/project/dbt/{args.version}/ exists '
                'and looks reasonable'
            )
        builder.upload(test=False)
    if args.build_homebrew:
        HomebrewBuilder(
            dbt_path=args.path,
            version=args.version,
            homebrew_path=args.homebrew_path,
            set_default=args.homebrew_set_default,
        ).build()


def main():
    sanity_check()
    args = Arguments.parse()
    upgrade_to(args)


if __name__ == '__main__':
    main()
