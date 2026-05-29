import os
import shutil
from unittest.mock import patch

import pytest

from dbt.tests.util import run_dbt

SKILLS_GIT_URL = "https://github.com/dbt-labs/dbt-agent-skills.git"
SKILLS_REVISION = "65d2e0b68e24b59e038e6deb14fa6624c63022fe"


class TestSkillDependencies:
    """Tests for skills installation via the `skills` key in dependencies.yml.

    Target behavior: `dbt deps` should install skill entries to the path
    specified by each skill's `path` field.
    """

    @pytest.fixture(scope="class")
    def dependencies(self):
        return {
            "skills": [
                {
                    "git": SKILLS_GIT_URL,
                    "revision": SKILLS_REVISION,
                    # path omitted — should default to .agents/skills
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_skills_not_present_before_deps(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        assert not os.path.exists(skills_dir)

    def test_skills_installed_after_deps(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        assert not os.path.exists(skills_dir)

        run_dbt(["deps"])

        assert os.path.exists(skills_dir), (
            "Expected .agents/skills/ to be created by `dbt deps` "
            "when dependencies.yml contains a skills entry with path: '.agents/skills'"
        )
        skill_subdir = os.path.join(skills_dir, "adding-dbt-unit-test")
        assert os.path.isdir(skill_subdir), (
            "Expected .agents/skills/adding-dbt-unit-test/ to exist, "
            f"but .agents/skills/ contains: {os.listdir(skills_dir)}"
        )

    def test_existing_skills_preserved_after_deps(self, project, clean_start):
        """Existing skill folders in the destination should not be wiped out
        when installing new skills from a different repo."""
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        # Pre-create an existing skill folder with a SKILL.md
        existing_skill = os.path.join(skills_dir, "my-custom-skill")
        os.makedirs(existing_skill, exist_ok=True)
        with open(os.path.join(existing_skill, "SKILL.md"), "w") as f:
            f.write("# My Custom Skill\n")

        run_dbt(["deps"])

        # The pre-existing skill should still be there
        assert os.path.isdir(existing_skill), (
            "Expected .agents/skills/my-custom-skill/ to survive `dbt deps`, "
            f"but .agents/skills/ contains: {os.listdir(skills_dir)}"
        )
        assert os.path.isfile(os.path.join(existing_skill, "SKILL.md"))

        # The newly installed skill should also be there
        new_skill = os.path.join(skills_dir, "adding-dbt-unit-test")
        assert os.path.isdir(new_skill), (
            "Expected .agents/skills/adding-dbt-unit-test/ to exist alongside "
            f"existing skills, but .agents/skills/ contains: {os.listdir(skills_dir)}"
        )


class TestSkillSubset:
    """When a `skills` list is specified on an entry, only those skills
    should be installed."""

    @pytest.fixture(scope="class")
    def dependencies(self):
        return {
            "skills": [
                {
                    "git": SKILLS_GIT_URL,
                    "revision": SKILLS_REVISION,
                    "path": ".agents/skills",
                    "skills": [
                        "adding-dbt-unit-test",
                        "running-dbt-commands",
                    ],
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_only_listed_skills_installed(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        run_dbt(["deps"])

        installed = set(os.listdir(skills_dir))
        assert "adding-dbt-unit-test" in installed
        assert "running-dbt-commands" in installed
        # A skill that exists in the repo but was NOT listed should be absent
        assert "fetching-dbt-docs" not in installed, (
            "Expected only the listed skills to be installed, " f"but found: {installed}"
        )


class TestLocalSkillDependencies:
    """Tests for installing skills from a local filesystem path."""

    @pytest.fixture(scope="class")
    def local_skills_dir(self, project_root):
        """Create a local directory with skill folders to install from."""
        local_src = os.path.join(project_root, "local_skills")
        # Create a valid skill (has SKILL.md)
        skill_a = os.path.join(local_src, "skill-alpha")
        os.makedirs(skill_a)
        with open(os.path.join(skill_a, "SKILL.md"), "w") as f:
            f.write("# Skill Alpha\n")
        with open(os.path.join(skill_a, "prompt.txt"), "w") as f:
            f.write("Do something useful\n")

        # Create another valid skill
        skill_b = os.path.join(local_src, "skill-beta")
        os.makedirs(skill_b)
        with open(os.path.join(skill_b, "SKILL.md"), "w") as f:
            f.write("# Skill Beta\n")

        # Create an invalid folder (no SKILL.md) — should NOT be installed
        not_a_skill = os.path.join(local_src, "not-a-skill")
        os.makedirs(not_a_skill)
        with open(os.path.join(not_a_skill, "README.md"), "w") as f:
            f.write("# Not a skill\n")

        return local_src

    @pytest.fixture(scope="class")
    def dependencies(self, local_skills_dir):
        return {
            "skills": [
                {
                    "local": "local_skills",
                    "path": ".agents/skills",
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_local_skills_installed(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        run_dbt(["deps"])

        installed = set(os.listdir(skills_dir))
        assert "skill-alpha" in installed
        assert "skill-beta" in installed
        # Folder without SKILL.md should not be installed
        assert (
            "not-a-skill" not in installed
        ), f"Folder without SKILL.md should not be installed, but found: {installed}"
        # Verify file contents survived the copy
        assert os.path.isfile(os.path.join(skills_dir, "skill-alpha", "prompt.txt"))


class TestLocalSkillWithSubdirectory:
    """Tests for installing local skills using a subdirectory override."""

    @pytest.fixture(scope="class")
    def local_skills_dir(self, project_root):
        """Create a local directory with skills nested in a subdirectory."""
        local_src = os.path.join(project_root, "my_repo", "nunchuck-skills")
        skill = os.path.join(local_src, "bo-staff")
        os.makedirs(skill)
        with open(os.path.join(skill, "SKILL.md"), "w") as f:
            f.write("# Bo Staff\n")
        return local_src

    @pytest.fixture(scope="class")
    def dependencies(self, local_skills_dir):
        return {
            "skills": [
                {
                    "local": "my_repo",
                    "subdirectory": "nunchuck-skills",
                    "path": ".agents/skills",
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_subdirectory_skills_installed(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        run_dbt(["deps"])

        assert os.path.isdir(
            os.path.join(skills_dir, "bo-staff")
        ), f".agents/skills/ contains: {os.listdir(skills_dir)}"


class TestMultipleInstallPaths:
    """The `path` field can be a list to install skills to multiple locations."""

    @pytest.fixture(scope="class")
    def local_skills_dir(self, project_root):
        local_src = os.path.join(project_root, "local_skills")
        skill = os.path.join(local_src, "my-skill")
        os.makedirs(skill)
        with open(os.path.join(skill, "SKILL.md"), "w") as f:
            f.write("# My Skill\n")
        return local_src

    @pytest.fixture(scope="class")
    def dependencies(self, local_skills_dir):
        return {
            "skills": [
                {
                    "local": "local_skills",
                    "path": [
                        ".agents/skills",
                        ".claude/skills",
                    ],
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        for d in [".agents/skills", ".claude/skills"]:
            full = os.path.join(project.project_root, d)
            if os.path.exists(full):
                shutil.rmtree(full)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_skills_installed_to_all_paths(self, project, clean_start):
        run_dbt(["deps"])

        for d in [".agents/skills", ".claude/skills"]:
            skill_dir = os.path.join(project.project_root, d, "my-skill")
            assert os.path.isdir(skill_dir), f"Expected {d}/my-skill/ to exist after `dbt deps`"
            assert os.path.isfile(os.path.join(skill_dir, "SKILL.md"))


class TestSingleSkillDirectory:
    """Case 3: the source itself is a skill directory (contains SKILL.md at its root)."""

    @pytest.fixture(scope="class")
    def local_skills_dir(self, project_root):
        # Create a single skill directory (not a parent containing skills)
        skill = os.path.join(project_root, "my-single-skill")
        os.makedirs(skill)
        with open(os.path.join(skill, "SKILL.md"), "w") as f:
            f.write("# My Single Skill\n")
        with open(os.path.join(skill, "prompt.txt"), "w") as f:
            f.write("Do something\n")
        return skill

    @pytest.fixture(scope="class")
    def dependencies(self, local_skills_dir):
        return {
            "skills": [
                {
                    "local": "my-single-skill",
                    "path": ".agents/skills",
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_single_skill_installed(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        run_dbt(["deps"])

        # The skill should be installed as a subdirectory named after the source
        skill_dest = os.path.join(skills_dir, "my-single-skill")
        assert os.path.isdir(skill_dest), (
            f"Expected .agents/skills/my-single-skill/ but found: "
            f"{os.listdir(skills_dir) if os.path.exists(skills_dir) else 'nothing'}"
        )
        assert os.path.isfile(os.path.join(skill_dest, "SKILL.md"))
        assert os.path.isfile(os.path.join(skill_dest, "prompt.txt"))


class TestSingleSkillTrailingSlash:
    """A local path with a trailing slash should still use the directory name."""

    @pytest.fixture(scope="class")
    def local_skills_dir(self, project_root):
        skill = os.path.join(project_root, "dinomight")
        os.makedirs(skill)
        with open(os.path.join(skill, "SKILL.md"), "w") as f:
            f.write("# Dinomight\n")
        return skill

    @pytest.fixture(scope="class")
    def dependencies(self, local_skills_dir):
        return {
            "skills": [
                {
                    "local": "dinomight/",
                    "path": ".agents/skills",
                }
            ]
        }

    @pytest.fixture
    def clean_start(self, project):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")
        if os.path.exists(skills_dir):
            shutil.rmtree(skills_dir)
        if os.path.exists("package-lock.yml"):
            os.remove("package-lock.yml")

    def test_trailing_slash_preserves_name(self, project, clean_start):
        skills_dir = os.path.join(project.project_root, ".agents", "skills")

        run_dbt(["deps"])

        skill_dest = os.path.join(skills_dir, "dinomight")
        assert os.path.isdir(skill_dest), (
            f"Expected .agents/skills/dinomight/ but found: "
            f"{os.listdir(skills_dir) if os.path.exists(skills_dir) else 'nothing'}"
        )
        assert os.path.isfile(os.path.join(skill_dest, "SKILL.md"))


class TestRegistrySkillPackageNotFound:
    """A bad package name in skills should produce a clear 'not found' error,
    not a raw ConnectionError with retry tracebacks."""

    @pytest.fixture(scope="class")
    def dependencies(self):
        return {
            "skills": [
                {
                    "package": "nonexistent-org/nonexistent-package",
                    "version": "1.0.0",
                }
            ]
        }

    def test_bad_package_raises_friendly_error(self, project):
        # Mock _fetch_metadata to simulate a registry 404, avoiding real
        # network calls and the slow 5-retry cycle.
        with patch(
            "dbt.deps.registry.RegistryPinnedPackage._fetch_metadata",
            side_effect=Exception("404 Client Error: Not Found"),
        ):
            with pytest.raises(Exception, match="not found in the package index"):
                run_dbt(["deps"])
