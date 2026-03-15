import os
import shutil

import pytest

from dbt.tests.util import run_dbt


class TestAddPackageWithWarnUnpinnedInYaml:
    """Functional test: Adding packages works even with warn-unpinned in packages.yml.

    This is a regression test for issue #9104. The bug occurred when packages.yml
    contained warn-unpinned: false and dbt deps --add-package was run. The code
    would fail with "TypeError: argument of type 'bool' is not iterable".
    """

    @pytest.fixture(scope="class")
    def packages(self):
        # Start with a git package that has warn-unpinned (matching the bug report)
        return {
            "packages": [
                {
                    "git": "https://github.com/fivetran/dbt_amplitude",
                    "warn-unpinned": False,  # This is the config that caused the bug
                },
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        if os.path.exists("dbt_packages"):
            shutil.rmtree("dbt_packages")
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_add_package_with_warn_unpinned_in_yaml(self, clean_start):
        """Test that adding a package works when packages.yml contains warn-unpinned: false"""
        # Before the fix, this would raise: TypeError: argument of type 'bool' is not iterable
        # This matches the exact scenario from issue #9104
        run_dbt(["deps", "--add-package", "dbt-labs/dbt_utils@1.0.0"])

        with open("packages.yml") as fp:
            contents = fp.read()

        # Verify both packages are present
        assert "dbt_amplitude" in contents or "fivetran/dbt_amplitude" in contents
        assert "dbt-labs/dbt_utils" in contents or "dbt_utils" in contents
        # The warn-unpinned should still be there
        assert "warn-unpinned:" in contents or "warn_unpinned:" in contents
