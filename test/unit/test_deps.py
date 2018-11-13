import unittest

import dbt.exceptions
from dbt.task.deps import GitPackage, LocalPackage


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

    def test_resovle_fail(self):
        a = GitPackage(git='http://example.com', revision='0.0.1')
        b = GitPackage(git='http://example.com', revision='0.0.2')
        c = a.incorporate(b)
        self.assertEqual(c.git, 'http://example.com')
        self.assertEqual(c.version, ['0.0.1', '0.0.2'])
        with self.assertRaises(dbt.exceptions.DependencyException):
            c.resolve_version()
