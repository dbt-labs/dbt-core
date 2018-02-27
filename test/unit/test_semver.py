import unittest
import itertools

from dbt.exceptions import VersionsNotCompatibleException
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier, \
    VersionRange, reduce_versions, versions_compatible, \
    resolve_to_specific_version


def create_range(start_version_string, end_version_string):
    start = UnboundedVersionSpecifier()
    end = UnboundedVersionSpecifier()

    if start_version_string is not None:
        start = VersionSpecifier.from_version_string(start_version_string)

    if end_version_string is not None:
        end = VersionSpecifier.from_version_string(end_version_string)

    return VersionRange(start=start, end=end)


class TestSemver(unittest.TestCase):

    def assertVersionSetResult(self, inputs, output_range):
        expected = create_range(*output_range)

        for permutation in itertools.permutations(inputs):
            self.assertDictEqual(
                reduce_versions(*permutation),
                expected)

    def assertInvalidVersionSet(self, inputs):
        for permutation in itertools.permutations(inputs):
            with self.assertRaises(VersionsNotCompatibleException):
                reduce_versions(*permutation)

    def test__versions_compatible(self):
        self.assertTrue(
            versions_compatible('0.0.1', '0.0.1'))
        self.assertFalse(
            versions_compatible('0.0.1', '0.0.2'))
        self.assertTrue(
            versions_compatible('>0.0.1', '0.0.2'))

    def test__reduce_versions(self):
        self.assertVersionSetResult(
            ['0.0.1', '0.0.1'],
            ['=0.0.1', '=0.0.1'])

        self.assertVersionSetResult(
            ['0.0.1'],
            ['=0.0.1', '=0.0.1'])

        self.assertVersionSetResult(
            ['>0.0.1'],
            ['>0.0.1', None])

        self.assertVersionSetResult(
            ['<0.0.1'],
            [None, '<0.0.1'])

        self.assertVersionSetResult(
            ['>0.0.1', '0.0.2'],
            ['=0.0.2', '=0.0.2'])

        self.assertVersionSetResult(
            ['0.0.2', '>=0.0.2'],
            ['=0.0.2', '=0.0.2'])

        self.assertVersionSetResult(
            ['>0.0.1', '>0.0.2', '>0.0.3'],
            ['>0.0.3', None])

        self.assertVersionSetResult(
            ['>0.0.1', '<0.0.3'],
            ['>0.0.1', '<0.0.3'])

        self.assertVersionSetResult(
            ['>0.0.1', '0.0.2', '<0.0.3'],
            ['=0.0.2', '=0.0.2'])

        self.assertVersionSetResult(
            ['>0.0.1', '>=0.0.1', '<0.0.3'],
            ['>0.0.1', '<0.0.3'])

        self.assertVersionSetResult(
            ['>0.0.1', '<0.0.3', '<=0.0.3'],
            ['>0.0.1', '<0.0.3'])

        self.assertVersionSetResult(
            ['>0.0.1', '>0.0.2', '<0.0.3', '<0.0.4'],
            ['>0.0.2', '<0.0.3'])

        self.assertVersionSetResult(
            ['<=0.0.3', '>=0.0.3'],
            ['>=0.0.3', '<=0.0.3'])

        self.assertInvalidVersionSet(['>0.0.2', '0.0.1'])
        self.assertInvalidVersionSet(['>0.0.2', '0.0.2'])
        self.assertInvalidVersionSet(['<0.0.2', '0.0.2'])
        self.assertInvalidVersionSet(['<0.0.2', '>0.0.3'])
        self.assertInvalidVersionSet(['<=0.0.3', '>0.0.3'])
        self.assertInvalidVersionSet(['<0.0.3', '>=0.0.3'])
        self.assertInvalidVersionSet(['<0.0.3', '>0.0.3'])

    def test__resolve_to_specific_version(self):
        self.assertEqual(
            resolve_to_specific_version(
                create_range('>0.0.1', None),
                ['0.0.1', '0.0.2']),
            '0.0.2')

        self.assertEqual(
            resolve_to_specific_version(
                create_range('>=0.0.2', None),
                ['0.0.1', '0.0.2']),
            '0.0.2')

        self.assertEqual(
            resolve_to_specific_version(
                create_range('>=0.0.3', None),
                ['0.0.1', '0.0.2']),
            None)

        self.assertEqual(
            resolve_to_specific_version(
                create_range('>=0.0.3', '<0.0.5'),
                ['0.0.3', '0.0.4', '0.0.5']),
            '0.0.4')

        self.assertEqual(
            resolve_to_specific_version(
                create_range(None, '<=0.0.5'),
                ['0.0.3', '0.1.4', '0.0.5']),
            '0.0.5')
