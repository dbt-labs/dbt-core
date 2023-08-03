import unittest
import itertools

from typing import List
from dbt.exceptions import VersionsNotCompatibleError
from dbt.semver import (
    VersionSpecifier,
    UnboundedVersionSpecifier,
    VersionRange,
    reduce_versions,
    versions_compatible,
    resolve_to_specific_version,
    filter_installable,
)


def semver_regex_versioning(versions: List[str]) -> bool:
    for version_string in versions:
        try:
            VersionSpecifier.from_version_string(version_string)
        except Exception:
            return False
    return True


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
            self.assertEqual(reduce_versions(*permutation), expected)

    def assertInvalidVersionSet(self, inputs):
        for permutation in itertools.permutations(inputs):
            with self.assertRaises(VersionsNotCompatibleError):
                reduce_versions(*permutation)

    def test__versions_compatible(self):
        self.assertTrue(versions_compatible("0.0.1", "0.0.1"))
        self.assertFalse(versions_compatible("0.0.1", "0.0.2"))
        self.assertTrue(versions_compatible(">0.0.1", "0.0.2"))
        self.assertFalse(versions_compatible("0.4.5a1", "0.4.5a2"))

    def test__semver_regex_versions(self):
        self.assertTrue(
            semver_regex_versioning(
                [
                    "0.0.4",
                    "1.2.3",
                    "10.20.30",
                    "1.1.2-prerelease+meta",
                    "1.1.2+meta",
                    "1.1.2+meta-valid",
                    "1.0.0-alpha",
                    "1.0.0-beta",
                    "1.0.0-alpha.beta",
                    "1.0.0-alpha.beta.1",
                    "1.0.0-alpha.1",
                    "1.0.0-alpha0.valid",
                    "1.0.0-alpha.0valid",
                    "1.0.0-alpha-a.b-c-somethinglong+build.1-aef.1-its-okay",
                    "1.0.0-rc.1+build.1",
                    "2.0.0-rc.1+build.123",
                    "1.2.3-beta",
                    "10.2.3-DEV-SNAPSHOT",
                    "1.2.3-SNAPSHOT-123",
                    "1.0.0",
                    "2.0.0",
                    "1.1.7",
                    "2.0.0+build.1848",
                    "2.0.1-alpha.1227",
                    "1.0.0-alpha+beta",
                    "1.2.3----RC-SNAPSHOT.12.9.1--.12+788",
                    "1.2.3----R-S.12.9.1--.12+meta",
                    "1.2.3----RC-SNAPSHOT.12.9.1--.12",
                    "1.0.0+0.build.1-rc.10000aaa-kk-0.1",
                    "99999999999999999999999.999999999999999999.99999999999999999",
                    "1.0.0-0A.is.legal",
                ]
            )
        )

        self.assertFalse(
            semver_regex_versioning(
                [
                    "1",
                    "1.2",
                    "1.2.3-0123",
                    "1.2.3-0123.0123",
                    "1.1.2+.123",
                    "+invalid",
                    "-invalid",
                    "-invalid+invalid",
                    "-invalid.01",
                    "alpha",
                    "alpha.beta",
                    "alpha.beta.1",
                    "alpha.1",
                    "alpha+beta",
                    "alpha_beta",
                    "alpha.",
                    "alpha..",
                    "beta",
                    "1.0.0-alpha_beta",
                    "-alpha.",
                    "1.0.0-alpha..",
                    "1.0.0-alpha..1",
                    "1.0.0-alpha...1",
                    "1.0.0-alpha....1",
                    "1.0.0-alpha.....1",
                    "1.0.0-alpha......1",
                    "1.0.0-alpha.......1",
                    "01.1.1",
                    "1.01.1",
                    "1.1.01",
                    "1.2",
                    "1.2.3.DEV",
                    "1.2-SNAPSHOT",
                    "1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788",
                    "1.2-RC-SNAPSHOT",
                    "-1.0.3-gamma+b7718",
                    "+justmeta",
                    "9.8.7+meta+meta",
                    "9.8.7-whatever+meta+meta",
                    "99999999999999999999999.999999999999999999.99999999999999999----RC-SNAPSHOT.12.09.1--------------------------------..12",
                ]
            )
        )

    def test__reduce_versions(self):
        self.assertVersionSetResult(["0.0.1", "0.0.1"], ["=0.0.1", "=0.0.1"])

        self.assertVersionSetResult(["0.0.1"], ["=0.0.1", "=0.0.1"])

        self.assertVersionSetResult([">0.0.1"], [">0.0.1", None])

        self.assertVersionSetResult(["<0.0.1"], [None, "<0.0.1"])

        self.assertVersionSetResult([">0.0.1", "0.0.2"], ["=0.0.2", "=0.0.2"])

        self.assertVersionSetResult(["0.0.2", ">=0.0.2"], ["=0.0.2", "=0.0.2"])

        self.assertVersionSetResult([">0.0.1", ">0.0.2", ">0.0.3"], [">0.0.3", None])

        self.assertVersionSetResult([">0.0.1", "<0.0.3"], [">0.0.1", "<0.0.3"])

        self.assertVersionSetResult([">0.0.1", "0.0.2", "<0.0.3"], ["=0.0.2", "=0.0.2"])

        self.assertVersionSetResult([">0.0.1", ">=0.0.1", "<0.0.3"], [">0.0.1", "<0.0.3"])

        self.assertVersionSetResult([">0.0.1", "<0.0.3", "<=0.0.3"], [">0.0.1", "<0.0.3"])

        self.assertVersionSetResult([">0.0.1", ">0.0.2", "<0.0.3", "<0.0.4"], [">0.0.2", "<0.0.3"])

        self.assertVersionSetResult(["<=0.0.3", ">=0.0.3"], [">=0.0.3", "<=0.0.3"])

        self.assertInvalidVersionSet([">0.0.2", "0.0.1"])
        self.assertInvalidVersionSet([">0.0.2", "0.0.2"])
        self.assertInvalidVersionSet(["<0.0.2", "0.0.2"])
        self.assertInvalidVersionSet(["<0.0.2", ">0.0.3"])
        self.assertInvalidVersionSet(["<=0.0.3", ">0.0.3"])
        self.assertInvalidVersionSet(["<0.0.3", ">=0.0.3"])
        self.assertInvalidVersionSet(["<0.0.3", ">0.0.3"])

    def test__resolve_to_specific_version(self):
        self.assertEqual(
            resolve_to_specific_version(create_range(">0.0.1", None), ["0.0.1", "0.0.2"]), "0.0.2"
        )

        self.assertEqual(
            resolve_to_specific_version(create_range(">=0.0.2", None), ["0.0.1", "0.0.2"]), "0.0.2"
        )

        self.assertEqual(
            resolve_to_specific_version(create_range(">=0.0.3", None), ["0.0.1", "0.0.2"]), None
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(">=0.0.3", "<0.0.5"), ["0.0.3", "0.0.4", "0.0.5"]
            ),
            "0.0.4",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(None, "<=0.0.5"), ["0.0.3", "0.1.4", "0.0.5"]
            ),
            "0.0.5",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range("=0.4.5a2", "=0.4.5a2"), ["0.4.5a1", "0.4.5a2"]
            ),
            "0.4.5a2",
        )

        self.assertEqual(
            resolve_to_specific_version(create_range("=0.7.6", "=0.7.6"), ["0.7.6-b1", "0.7.6"]),
            "0.7.6",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(">=1.0.0", None), ["1.0.0", "1.1.0a1", "1.1.0", "1.2.0a1"]
            ),
            "1.2.0a1",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(">=1.0.0", "<1.2.0"), ["1.0.0", "1.1.0a1", "1.1.0", "1.2.0a1"]
            ),
            "1.1.0",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(">=1.0.0", None), ["1.0.0", "1.1.0a1", "1.1.0", "1.2.0a1", "1.2.0"]
            ),
            "1.2.0",
        )

        self.assertEqual(
            resolve_to_specific_version(
                create_range(">=1.0.0", "<1.2.0"),
                ["1.0.0", "1.1.0a1", "1.1.0", "1.2.0a1", "1.2.0"],
            ),
            "1.1.0",
        )

        self.assertEqual(
            resolve_to_specific_version(
                # https://github.com/dbt-labs/dbt-core/issues/7039
                # 10 is greater than 9
                create_range(">0.9.0", "<0.10.0"),
                ["0.9.0", "0.9.1", "0.10.0"],
            ),
            "0.9.1",
        )

    def test__filter_installable(self):
        installable = filter_installable(
            [
                "1.1.0",
                "1.2.0a1",
                "1.0.0",
                "2.1.0-alpha",
                "2.2.0asdf",
                "2.1.0",
                "2.2.0",
                "2.2.0-fishtown-beta",
                "2.2.0-2",
            ],
            install_prerelease=True,
        )
        expected = [
            "1.0.0",
            "1.1.0",
            "1.2.0a1",
            "2.1.0-alpha",
            "2.1.0",
            "2.2.0-2",
            "2.2.0asdf",
            "2.2.0-fishtown-beta",
            "2.2.0",
        ]
        assert installable == expected

        installable = filter_installable(
            [
                "1.1.0",
                "1.2.0a1",
                "1.0.0",
                "2.1.0-alpha",
                "2.2.0asdf",
                "2.1.0",
                "2.2.0",
                "2.2.0-fishtown-beta",
            ],
            install_prerelease=False,
        )
        expected = ["1.0.0", "1.1.0", "2.1.0", "2.2.0"]
        assert installable == expected
