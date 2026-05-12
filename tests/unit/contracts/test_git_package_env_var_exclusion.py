import json
from hashlib import sha1

from dbt.contracts.project import GitPackage
from dbt.task.deps import _create_sha1_hash
from tests.unit.utils import ContractTestCase


class TestGitPackageEnvVarExclusion(ContractTestCase):
    ContractType = GitPackage

    def test_git_package_with_exclude_env_vars_from_hash_true(self):
        """Test GitPackage with exclude-env-vars-from-hash set to True"""
        dct = {
            "git": "https://github.com/user/repo.git",
            "revision": "v1.0.0",
            "exclude-env-vars-from-hash": True,
        }
        package = self.ContractType(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
        )
        self.assert_from_dict(package, dct)

    def test_git_package_with_exclude_env_vars_from_hash_false(self):
        """Test GitPackage with exclude-env-vars-from-hash set to False"""
        dct = {
            "git": "https://github.com/user/repo.git",
            "revision": "v1.0.0",
            "exclude-env-vars-from-hash": False,
        }
        package = self.ContractType(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=False,
        )
        self.assert_from_dict(package, dct)

    def test_git_package_without_exclude_env_vars_from_hash(self):
        """Test GitPackage without exclude-env-vars-from-hash (default behavior)"""
        dct = {
            "git": "https://github.com/user/repo.git",
            "revision": "v1.0.0",
        }
        package = self.ContractType(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
        )
        self.assert_from_dict(package, dct)

    def test_to_dict_for_hash_excludes_env_vars_when_flag_is_true(self):
        """Test that to_dict_for_hash excludes env vars when exclude_env_vars_from_hash is True"""
        # Create a package with unrendered git URL containing env vars
        package = GitPackage(
            git="https://github.com/${GITHUB_USER}/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        # Get the dict for hash calculation
        hash_dict = package.to_dict_for_hash()

        # Should use the unrendered git URL and exclude the flag itself
        self.assertEqual(hash_dict["git"], "https://github.com/${GITHUB_USER}/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertNotIn("exclude-env-vars-from-hash", hash_dict)

    def test_to_dict_for_hash_includes_env_vars_when_flag_is_false(self):
        """Test that to_dict_for_hash includes env vars when exclude_env_vars_from_hash is False"""
        # Create a package with rendered git URL
        package = GitPackage(
            git="https://github.com/actualuser/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=False,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        # Get the dict for hash calculation
        hash_dict = package.to_dict_for_hash()

        # Should use the rendered git URL and include the flag
        self.assertEqual(hash_dict["git"], "https://github.com/actualuser/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertEqual(hash_dict["exclude-env-vars-from-hash"], False)

    def test_to_dict_for_hash_includes_env_vars_when_flag_is_none(self):
        """Test that to_dict_for_hash includes env vars when exclude_env_vars_from_hash is None (default)"""
        # Create a package with rendered git URL
        package = GitPackage(
            git="https://github.com/actualuser/repo.git",
            revision="v1.0.0",
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        # Get the dict for hash calculation (should use regular to_dict)
        hash_dict = package.to_dict_for_hash()

        # Should use the rendered git URL since flag is None (falsy)
        self.assertEqual(hash_dict["git"], "https://github.com/actualuser/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertIsNone(hash_dict.get("exclude-env-vars-from-hash"))

    def test_hash_calculation_with_exclude_env_vars_true(self):
        """Test that _create_sha1_hash uses to_dict_for_hash when available and excludes env vars"""
        # Create two packages: one with env vars resolved, one without
        package_with_env_vars = GitPackage(
            git="https://github.com/actualuser/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        package_without_env_vars = GitPackage(
            git="https://github.com/differentuser/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        # Both packages should produce the same hash since they use unrendered URLs
        hash1 = _create_sha1_hash([package_with_env_vars])
        hash2 = _create_sha1_hash([package_without_env_vars])

        self.assertEqual(hash1, hash2, "Hashes should be identical when using unrendered URLs")

    def test_hash_calculation_with_exclude_env_vars_false(self):
        """Test that _create_sha1_hash includes env vars when exclude_env_vars_from_hash is False"""
        # Create two packages with different resolved git URLs
        package1 = GitPackage(
            git="https://github.com/user1/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=False,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        package2 = GitPackage(
            git="https://github.com/user2/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=False,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        # These packages should produce different hashes since they use rendered URLs
        hash1 = _create_sha1_hash([package1])
        hash2 = _create_sha1_hash([package2])

        self.assertNotEqual(hash1, hash2, "Hashes should be different when using rendered URLs")

    def test_hash_calculation_backwards_compatibility(self):
        """Test that packages without to_dict_for_hash method still work (backwards compatibility)"""

        # Create a simple mock package without to_dict_for_hash method
        class SimplePackage:
            def to_dict(self):
                return {"git": "https://github.com/user/repo.git", "revision": "v1.0.0"}

        simple_package = SimplePackage()

        # This should not raise an error and should use to_dict instead
        hash_result = _create_sha1_hash([simple_package])

        # Verify the hash is calculated correctly
        expected_dict = {"git": "https://github.com/user/repo.git", "revision": "v1.0.0"}
        expected_str = json.dumps(expected_dict, sort_keys=True)
        expected_hash = sha1(expected_str.encode("utf-8")).hexdigest()

        self.assertEqual(hash_result, expected_hash)

    def test_package_with_subdirectory_and_exclude_env_vars(self):
        """Test GitPackage with subdirectory and exclude-env-vars-from-hash"""
        package = GitPackage(
            git="https://github.com/actualuser/repo.git",
            revision="v1.0.0",
            subdirectory="subdir",
            exclude_env_vars_from_hash=True,
            unrendered={"git": "https://github.com/${GITHUB_USER}/repo.git"},
        )

        hash_dict = package.to_dict_for_hash()

        # Should include subdirectory and use unrendered git URL
        self.assertEqual(hash_dict["git"], "https://github.com/${GITHUB_USER}/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertEqual(hash_dict["subdirectory"], "subdir")
        self.assertNotIn("exclude-env-vars-from-hash", hash_dict)

    def test_package_without_unrendered_git_url(self):
        """Test GitPackage with exclude_env_vars_from_hash=True but no unrendered git URL"""
        package = GitPackage(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            # No unrendered dict or git key
        )

        hash_dict = package.to_dict_for_hash()

        # Should use the regular git URL since no unrendered version is available
        self.assertEqual(hash_dict["git"], "https://github.com/user/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertNotIn("exclude-env-vars-from-hash", hash_dict)

    def test_yaml_alias_support(self):
        """Test that the YAML alias 'exclude-env-vars-from-hash' works correctly"""
        # Test creating from dict using the alias
        dct_with_alias = {
            "git": "https://github.com/user/repo.git",
            "revision": "v1.0.0",
            "exclude-env-vars-from-hash": True,
        }
        package = GitPackage.from_dict(dct_with_alias)

        # The field should be accessible via the Python attribute name
        self.assertTrue(package.exclude_env_vars_from_hash)

        # Converting back to dict should use the alias
        result_dict = package.to_dict()
        self.assertIn("exclude-env-vars-from-hash", result_dict)
        self.assertTrue(result_dict["exclude-env-vars-from-hash"])

    def test_hash_consistency_across_environments(self):
        """Test that packages with same unrendered URLs produce consistent hashes"""
        # Simulate packages from different environments with different resolved URLs
        package_env1 = GitPackage(
            git="https://github.com/user1/repo.git",  # Different resolved URLs
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={
                "git": "https://github.com/${GITHUB_USER}/repo.git"
            },  # Same unrendered URL
        )

        package_env2 = GitPackage(
            git="https://github.com/user2/repo.git",  # Different resolved URLs
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={
                "git": "https://github.com/${GITHUB_USER}/repo.git"
            },  # Same unrendered URL
        )

        # Both should produce the same hash
        hash1 = _create_sha1_hash([package_env1])
        hash2 = _create_sha1_hash([package_env2])

        self.assertEqual(
            hash1, hash2, "Packages with same unrendered URLs should have consistent hashes"
        )

    def test_mixed_package_types_in_hash(self):
        """Test hash calculation with mixed package types (some with to_dict_for_hash, some without)"""
        # GitPackage with to_dict_for_hash method
        git_package = GitPackage(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={"git": "https://github.com/${USER}/repo.git"},
        )

        # Mock package without to_dict_for_hash method
        class SimplePackage:
            def to_dict(self):
                return {"tarball": "https://example.com/package.tar.gz", "name": "simple_package"}

        simple_package = SimplePackage()

        # Should handle mixed package types without error
        hash_result = _create_sha1_hash([git_package, simple_package])

        # Verify it's a valid SHA1 hash (40 hex characters)
        self.assertEqual(len(hash_result), 40)
        self.assertTrue(all(c in "0123456789abcdef" for c in hash_result))

    def test_empty_unrendered_dict(self):
        """Test GitPackage with exclude_env_vars_from_hash=True but empty unrendered dict"""
        package = GitPackage(
            git="https://github.com/user/repo.git",
            revision="v1.0.0",
            exclude_env_vars_from_hash=True,
            unrendered={},  # Empty dict, no git key
        )

        hash_dict = package.to_dict_for_hash()

        # Should use the regular git URL since unrendered["git"] doesn't exist
        self.assertEqual(hash_dict["git"], "https://github.com/user/repo.git")
        self.assertEqual(hash_dict["revision"], "v1.0.0")
        self.assertNotIn("exclude-env-vars-from-hash", hash_dict)
