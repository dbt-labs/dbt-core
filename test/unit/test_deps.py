import unittest
import mock

import dbt.exceptions
from dbt.task.deps import GitPackage, LocalPackage, RegistryPackage
from dbt.semver import VersionSpecifier

class TestLocalPackage(unittest.TestCase):
    def test_init(self):
        a = LocalPackage(local='/path/to/package')
        a.resolve_version()
        self.assertEqual(a.source_type(), 'local')
        self.assertEqual(a.local, '/path/to/package')


class TestGitPackage(unittest.TestCase):
    def test_init(self):
        a = GitPackage(git='http://example.com', revision='0.0.1')
        self.assertEqual(a.git, 'http://example.com')
        self.assertEqual(a.revision, '0.0.1')
        self.assertEqual(a.version, ['0.0.1'])
        self.assertEqual(a.source_type(), 'git')

    def test_invalid(self):
        with self.assertRaises(dbt.exceptions.ValidationException):
            GitPackage(git='http://example.com', version='0.0.1')

    def test_resolve_ok(self):
        a = GitPackage(git='http://example.com', revision='0.0.1')
        b = GitPackage(git='http://example.com', revision='0.0.1')
        c = a.incorporate(b)
        self.assertEqual(c.git, 'http://example.com')
        self.assertEqual(c.version, ['0.0.1', '0.0.1'])
        c.resolve_version()
        self.assertEqual(c.version, ['0.0.1'])

    def test_resolve_fail(self):
        a = GitPackage(git='http://example.com', revision='0.0.1')
        b = GitPackage(git='http://example.com', revision='0.0.2')
        c = a.incorporate(b)
        self.assertEqual(c.git, 'http://example.com')
        self.assertEqual(c.version, ['0.0.1', '0.0.2'])
        with self.assertRaises(dbt.exceptions.DependencyException):
            c.resolve_version()


class TestHubPackage(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch('dbt.task.deps.registry')
        self.registry = self.patcher.start()
        self.index_cached = self.registry.index_cached
        self.get_available_versions = self.registry.get_available_versions
        self.package_version = self.registry.package_version

    def tearDown(self):
        self.patcher.stop()

    def test_init(self):
        a = RegistryPackage(package='fishtown-analytics-test/a',
                            version='0.1.2')
        self.assertEqual(a.package, 'fishtown-analytics-test/a')
        self.assertEqual(
            a.version,
            [VersionSpecifier(
                build=None,
                major='0',
                matcher='=',
                minor='1',
                patch='2',
                prerelease=None
            )]
        )
        self.assertEqual(a.source_type(), 'hub')

    def test_invalid(self):
        with self.assertRaises(dbt.exceptions.ValidationException):
            RegistryPackage(package='namespace/name', key='invalid')

    def test_resolve_ok(self):
        self.index_cached.return_value = [
            'fishtown-analytics-test/a',
        ]
        self.get_available_versions.return_value = [
            '0.1.2', '0.1.3'
        ]
        self.package_version.return_value = {
            'id': 'fishtown-analytics-test/a/0.1.2',
            'name': 'a',
            'version': '0.1.2',
            'packages': [],
            '_source': {
                'blahblah': 'asdfas',
            },
            'downloads': {
                'tarball': 'https://example.com/invalid-url!',
                'extra': 'field',
            },
            'newfield': ['another', 'value'],
        }

        a = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.2'
        )
        b = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.2'
        )
        c = a.incorporate(b)
        self.assertEqual(
            c.version,
            [
                VersionSpecifier({
                    'build': None,
                    'major': '0',
                    'matcher': '=',
                    'minor': '1',
                    'patch': '2',
                    'prerelease': None,
                }),
                VersionSpecifier({
                    'build': None,
                    'major': '0',
                    'matcher': '=',
                    'minor': '1',
                    'patch': '2',
                    'prerelease': None,
                })
            ]
        )
        c.resolve_version()
        self.assertEqual(c.package, 'fishtown-analytics-test/a')
        self.assertEqual(
            c.version,
            [VersionSpecifier({
                'build': None,
                'major': '0',
                'matcher': '=',
                'minor': '1',
                'patch': '2',
                'prerelease': None,
            })]
        )
        self.assertEqual(c.source_type(), 'hub')

    def test_resolve_missing_package(self):
        self.index_cached.return_value = [
            'fishtown-analytics-test/b',
        ]
        a = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.2'
        )
        with self.assertRaises(dbt.exceptions.DependencyException) as e:
            exc = e
            a.resolve_version()

        msg = 'Package fishtown-analytics-test/a was not found in the package index'
        self.assertEqual(msg, str(exc.exception))

    def test_resolve_missing_version(self):
        self.index_cached.return_value = [
            'fishtown-analytics-test/a',
        ]
        self.get_available_versions.return_value = [
            '0.1.3', '0.1.4'
        ]
        a = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.2'
        )
        with self.assertRaises(dbt.exceptions.DependencyException) as e:
            exc = e
            a.resolve_version()
        msg = (
            "Could not find a matching version for package "
            "fishtown-analytics-test/a\n  Requested range: =0.1.2, =0.1.2\n  "
            "Available versions: ['0.1.3', '0.1.4']"
        )
        self.assertEqual(msg, str(exc.exception))

    def test_resolve_conflict(self):
        self.index_cached.return_value = [
            'fishtown-analytics-test/a',
        ]
        self.get_available_versions.return_value = [
            '0.1.2', '0.1.3'
        ]
        a = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.2'
        )
        b = RegistryPackage(
            package='fishtown-analytics-test/a',
            version='0.1.3'
        )
        c = a.incorporate(b)
        with self.assertRaises(dbt.exceptions.DependencyException) as e:
            exc = e
            c.resolve_version()
        msg = (
            "Version error for package fishtown-analytics-test/a: Could not "
            "find a satisfactory version from options: ['=0.1.2', '=0.1.3']"
        )
        self.assertEqual(msg, str(exc.exception))
