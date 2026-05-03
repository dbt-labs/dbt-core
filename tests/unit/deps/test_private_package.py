import json

import pytest

from dbt.deps.private_package import (
    ADOLegacyPrivatePackageName,
    GitProvider,
    GroupName,
    OrgName,
    PrivatePackageDefinition,
    PrivatePackageHelper,
    PrivatePackageName,
    PrivatePackageResolutionError,
    RepoName,
)


# -----------------------------------------------------------------------------
# Shared fixtures (git repository configs as list[dict])
# -----------------------------------------------------------------------------
@pytest.fixture
def git_repositories_github() -> list[dict]:
    return [
        {
            "org": "github-labs",
            "url": "https://{token}@github.com/github-labs/{repo}.git",
            "token": "github_token",
            "provider": "github",
        }
    ]


@pytest.fixture
def git_repositories_gitlab_2_part() -> list[dict]:
    """Fixed repo for A1: only gitlab-labs/test matches."""
    return [
        {
            "org": "gitlab-labs",
            "url": "https://{token}@gitlab.com/gitlab-labs/test.git",
            "token": "gitlab_token",
            "provider": "gitlab",
        }
    ]


@pytest.fixture
def git_repositories_gitlab_n_part() -> list[dict]:
    """Fixed repo for B2: only gitlab-labs/gitlab-project/test matches."""
    return [
        {
            "org": "gitlab-labs",
            "url": "https://{token}@gitlab.com/gitlab-labs/gitlab-project/test.git",
            "token": "gitlab_group_token",
            "provider": "gitlab",
        }
    ]


@pytest.fixture
def git_repositories_aad() -> list[dict]:
    return [
        {
            "org": "aad-labs",
            "url": "https://{token}@dev.azure.com/aad-labs/aad-project/_git/{repo}.git",
            "token": "aad_token",
            "provider": "azure_active_directory",
        }
    ]


@pytest.fixture
def git_repositories_ado() -> list[dict]:
    return [
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
            "token": "ado_token",
            "provider": "ado",
        }
    ]


@pytest.fixture
def git_repositories_ado_with_legacy() -> list[dict]:
    """B1: ado (3-part) + azure_active_directory (2-part legacy) for same URL."""
    return [
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
            "token": "ado_token",
            "provider": "azure_active_directory",
        },
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
            "token": "ado_token",
            "provider": "ado",
        },
    ]


@pytest.fixture
def git_repositories_ado_wildcard() -> list[dict]:
    """New org-level ADO format: one entry per org with {project} wildcard."""
    return [
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/{project}/_git/{repo}.git",
            "token": "ado_token",
            "provider": "ado",
        }
    ]


@pytest.fixture
def git_repositories_multi_provider(
    git_repositories_github: list[dict],
    git_repositories_gitlab_2_part: list[dict],
    git_repositories_gitlab_n_part: list[dict],
    git_repositories_aad: list[dict],
    git_repositories_ado: list[dict],
) -> list[dict]:
    """Multi-provider config: GitHub, GitLab, AAD (legacy), ADO (full) + second ADO project + ado legacy for 2-part."""
    ado_other_project = [
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/other-project/_git/{repo}.git",
            "token": "ado_token_2",
            "provider": "ado",
        }
    ]
    ado_legacy = [
        {
            "org": "ado-labs",
            "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
            "token": "ado_token",
            "provider": "azure_active_directory",
        }
    ]
    return [
        *git_repositories_github,
        *git_repositories_gitlab_2_part,
        *git_repositories_gitlab_n_part,
        *git_repositories_aad,
        *git_repositories_ado,
        *ado_other_project,
        *ado_legacy,
    ]


# -----------------------------------------------------------------------------
# TestGitProvider: enum values and ADO equivalence
# -----------------------------------------------------------------------------


class TestGitProvider:
    def test_ado_variants_equivalent_to_each_other(self):
        assert GitProvider.AZURE_DEVOPS == GitProvider.AZURE_ACTIVE_DIRECTORY
        assert GitProvider.AZURE_ACTIVE_DIRECTORY == GitProvider.AZURE_DEVOPS

    def test_non_ado_providers_distinct(self):
        assert GitProvider.GITHUB != GitProvider.GITLAB
        assert GitProvider.GITHUB != "gitlab"
        assert GitProvider.GITLAB != "github"

    def test_ado_does_not_match_non_ado(self):
        assert GitProvider.AZURE_DEVOPS != GitProvider.GITHUB
        assert GitProvider.AZURE_DEVOPS != "github"
        assert GitProvider.AZURE_ACTIVE_DIRECTORY != "gitlab"

    def test_ado_equivalent_to_legacy_providers(self):
        """ADO, azure_active_directory, and azure_devops are equivalent for provider matching."""
        assert GitProvider.ADO == GitProvider.AZURE_DEVOPS
        assert GitProvider.ADO == GitProvider.AZURE_ACTIVE_DIRECTORY
        assert GitProvider.ADO == "azure_devops"
        assert GitProvider.ADO == "azure_active_directory"

    def test_ne_negates_eq(self):
        assert GitProvider.GITHUB != "gitlab"
        assert GitProvider.AZURE_DEVOPS != "github"


# -----------------------------------------------------------------------------
# TestGroupName: wildcard and equality
# -----------------------------------------------------------------------------


class TestGroupName:
    def test_wildcard_is_true_for_placeholder(self):
        assert GroupName("{group}").is_wildcard is True

    def test_wildcard_is_false_for_concrete_name(self):
        assert GroupName("my-group").is_wildcard is False

    def test_equality_when_one_is_wildcard(self):
        assert GroupName("{group}") == GroupName("my-group")
        assert GroupName("my-group") == GroupName("{group}")

    def test_equality_when_neither_is_wildcard(self):
        assert GroupName("group-a") == GroupName("group-a")
        assert GroupName("group-a") != GroupName("group-b")

    def test_equality_with_plain_str(self):
        """GroupName wraps plain str for comparison."""
        assert GroupName("my-group") == "my-group"
        assert GroupName("my-group") != "other-group"

    def test_wildcard_does_not_match_empty(self):
        """Wildcard requires non-empty on the other side (cannot interpolate empty)."""
        assert GroupName("{group}") != GroupName("")
        assert GroupName("") != GroupName("{group}")


class TestRepoName:
    """RepoName wildcard ({repo}) behavior."""

    def test_wildcard_matches_non_empty(self):
        assert RepoName("{repo}") == RepoName("test")
        assert RepoName("test") == RepoName("{repo}")

    def test_wildcard_does_not_match_empty(self):
        assert RepoName("{repo}") != RepoName("")
        assert RepoName("") != RepoName("{repo}")

    def test_concrete_repos_match(self):
        assert RepoName("my-repo") == RepoName("my-repo")
        assert RepoName("my-repo") != RepoName("other-repo")


class TestOrgName:
    """OrgName has no wildcard support."""

    def test_is_wildcard_is_false(self):
        assert OrgName("dbt-labs").is_wildcard is False

    def test_equality_is_strict(self):
        assert OrgName("dbt-labs") == OrgName("dbt-labs")
        assert OrgName("dbt-labs") != OrgName("other-org")


# -----------------------------------------------------------------------------
# TestPrivatePackageName: extraction and equality
# -----------------------------------------------------------------------------


class TestPrivatePackageName:
    def test_extracts_org_groups_repo_from_three_part_path(self):
        name = PrivatePackageName("dbt-labs/my-group/my-repo")
        assert str(name.org_name) == "dbt-labs"
        assert str(name.group) == "my-group"
        assert str(name.repo_name) == "my-repo"

    def test_extracts_org_and_repo_from_two_part_path(self):
        name = PrivatePackageName("dbt-labs/repo")
        assert str(name.org_name) == "dbt-labs"
        assert str(name.group) == ""
        assert str(name.repo_name) == "repo"

    def test_extracts_multi_segment_group(self):
        name = PrivatePackageName("ado-labs/team/subproject/repo")
        assert str(name.org_name) == "ado-labs"
        assert str(name.group) == "team/subproject"
        assert str(name.repo_name) == "repo"

    def test_equality_compares_org_groups_repo(self):
        a = PrivatePackageName("dbt-labs/my-group/repo")
        b = PrivatePackageName("dbt-labs/my-group/repo")
        assert a == b

    def test_inequality_when_groups_differ(self):
        a = PrivatePackageName("dbt-labs/my-group/repo")
        b = PrivatePackageName("dbt-labs/your-group/repo")
        assert a != b


class TestPrivatePackageDefinition:
    def test_build_raises_for_invalid_provider(self):
        with pytest.raises(PrivatePackageResolutionError) as exc_info:
            PrivatePackageDefinition.build(name="org/repo", provider="bitbucket")
        assert "bitbucket" in str(exc_info.value)
        assert "Valid providers:" in str(exc_info.value)
        assert "github" in str(exc_info.value)


class TestADOLegacyPrivatePackageName:
    def test_equality_ignores_groups(self):
        a = ADOLegacyPrivatePackageName("ado-labs/project-a/repo")
        b = ADOLegacyPrivatePackageName("ado-labs/project-b/repo")
        assert a.org_name == b.org_name
        assert a.repo_name == b.repo_name
        assert a == b

    def test_inequality_with_groups_when_org_differs(self):
        a = ADOLegacyPrivatePackageName("ado-labs/project-a/repo")
        b = ADOLegacyPrivatePackageName("other-org/project-a/repo")
        assert a != b

    def test_inequality_with_groups_when_repo_differs(self):
        a = ADOLegacyPrivatePackageName("ado-labs/project-a/repo-a")
        b = ADOLegacyPrivatePackageName("ado-labs/project-a/repo-b")
        assert a != b

    def test_inequality_without_groups_when_org_differs(self):
        a = ADOLegacyPrivatePackageName("ado-labs/repo")
        b = ADOLegacyPrivatePackageName("other-org/repo")
        assert a != b

    def test_inequality_without_groups_when_repo_differs(self):
        a = ADOLegacyPrivatePackageName("ado-labs/repo-a")
        b = ADOLegacyPrivatePackageName("ado-labs/repo-b")
        assert a != b


# -----------------------------------------------------------------------------
# TestPrivatePackageHelper: resolution (provider-specific) and multi-provider
# -----------------------------------------------------------------------------


class BasePrivatePackageHelperResolution:
    """Base class with resolve() helper for resolution tests."""

    def resolve(
        self,
        git_providers: list[dict],
        package_name: str,
        provider: str | None = None,
    ) -> str:
        git_providers_json = json.dumps(git_providers)
        helper = PrivatePackageHelper(git_providers_json)
        return helper.get_resolved_url(
            private_def=package_name,
            provider=provider,
        )


# -----------------------------------------------------------------------------
# GITHUB Private Package Use Cases
# -----------------------------------------------------------------------------
class TestGitHubPrivatePackageResolution(BasePrivatePackageHelperResolution):
    """
    Valid dbt-cloud formats:
    - (A) 2-part github: ORG/{REPO} github

    Valid core package formats only:
    - (1) 2-part github: ORG/REPO github

    Expected use cases:
        ID | dbt-cloud                  | core (packages.yml)          | Expected
        ---|----------------------------|------------------------------|---------
        A1 | ORG/{REPO} github          | ORG/REPO github              | OK
    """

    def test_use_case_A1(self, git_repositories_github: list[dict]) -> None:
        """A1: ORG/{REPO} github + ORG/REPO github"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_github,
            package_name="github-labs/test",
            provider="github",
        )
        expected = "https://github_token@github.com/github-labs/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_github,
            package_name="github-labs/test",
        )
        expected = "https://github_token@github.com/github-labs/test.git"
        assert resolved == expected

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_github,
                package_name="other-org/test",
                provider="github",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_github,
                package_name="github-labs/test",
                provider="gitlab",
            )

        """Azure DevOps: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_github,
                package_name="github-labs/test",
                provider="azure_devops",
            )

        """ADO: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_github,
                package_name="github-labs/test",
                provider="ado",
            )


# -----------------------------------------------------------------------------
# GITLAB Private Package Use Cases
# -----------------------------------------------------------------------------
class TestGitLabPrivatePackageResolution(BasePrivatePackageHelperResolution):
    """
    Valid dbt-cloud formats:
    - (A) 2-part gitlab: ORG/GROUP gitlab
    - (B) N-part gitlab: ORG/GROUP/REPO gitlab

    Valid core package formats:
    - (1) 2-part gitlab: ORG/REPO gitlab
    - (2) N-part gitlab: ORG/GROUP/REPO gitlab

    Expected use cases:
        ID | dbt-cloud                  | core (packages.yml)          | Expected
        ---|----------------------------|------------------------------|---------
        A1 | ORG/GROUP gitlab           | ORG/REPO gitlab              | OK
        A2 | ORG/GROUP/REPO gitlab      | ORG/GROUP/REPO gitlab        | FAIL
        B1 | ORG/REPO gitlab            | ORG/REPO gitlab              | FAIL
        B2 | ORG/GROUP/REPO gitlab      | ORG/GROUP/REPO gitlab        | OK
    """

    def test_use_case_A1(self, git_repositories_gitlab_2_part: list[dict]) -> None:
        """A1: ORG/GROUP gitlab + ORG/REPO gitlab"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_gitlab_2_part,
            package_name="gitlab-labs/test",
            provider="gitlab",
        )
        expected = "https://gitlab_token@gitlab.com/gitlab-labs/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_gitlab_2_part,
            package_name="gitlab-labs/test",
        )
        expected = "https://gitlab_token@gitlab.com/gitlab-labs/test.git"
        assert resolved == expected

        """Another repo: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/another-test",
                provider="gitlab",
            )

        """Another group: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/other-group/test",
                provider="gitlab",
            )

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="other-org/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/test",
                provider="github",
            )

        """Azure DevOps: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/test",
                provider="azure_devops",
            )

        """ADO: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/test",
                provider="ado",
            )

    def test_use_case_A2(self, git_repositories_gitlab_2_part: list[dict]) -> None:
        """A2: ORG/GROUP/REPO gitlab + ORG/GROUP/REPO gitlab"""

        """With provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/gitlab-project/test",
                provider="gitlab",
            )

        """Without provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_2_part,
                package_name="gitlab-labs/gitlab-project/test",
            )

    def test_use_case_B1(self, git_repositories_gitlab_n_part: list[dict]) -> None:
        """B1: ORG/REPO gitlab + ORG/REPO gitlab"""

        """With provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/test",
                provider="gitlab",
            )

        """Without provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/test",
            )

    def test_use_case_B2(self, git_repositories_gitlab_n_part: list[dict]) -> None:
        """B2: ORG/GROUP/REPO gitlab + ORG/GROUP/REPO gitlab"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_gitlab_n_part,
            package_name="gitlab-labs/gitlab-project/test",
            provider="gitlab",
        )
        expected = "https://gitlab_group_token@gitlab.com/gitlab-labs/gitlab-project/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_gitlab_n_part,
            package_name="gitlab-labs/gitlab-project/test",
        )
        expected = "https://gitlab_group_token@gitlab.com/gitlab-labs/gitlab-project/test.git"
        assert resolved == expected

        """Another repo: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/gitlab-project/another-test",
                provider="gitlab",
            )

        """Another group: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/other-project/test",
                provider="gitlab",
            )

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="other-org/gitlab-project/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/gitlab-project/test",
                provider="github",
            )

        """Azure DevOps: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/gitlab-project/test",
                provider="azure_devops",
            )

        """ADO: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_gitlab_n_part,
                package_name="gitlab-labs/gitlab-project/test",
                provider="ado",
            )


# -----------------------------------------------------------------------------
# AZURE Private Package Use Cases
# -----------------------------------------------------------------------------
class TestAzurePrivatePackageResolution(BasePrivatePackageHelperResolution):
    """
    Valid dbt-cloud formats:
    - (A) N-part azure_active_directory: ORG/PROJECT/{REPO}
    - (B) N-part ado: ORG/PROJECT/{REPO}

    Valid core package formats:
    - (1) 2-part ado: ORG/REPO azure_devops
    - (2) 2-part azure_devops: ORG/REPO ado
    - (3) N-part ado: ORG/GROUP/REPO azure_devops
    - (4) N-part azure_devops: ORG/GROUP/REPO ado

    Expected use cases:

        ID | dbt-cloud                                     | core (packages.yml)          | Expected
        ---|-----------------------------------------------|------------------------------|---------
        A1 | ORG/PROJECT/{REPO} azure_active_directory     | ORG/REPO azure_devops        | OK
        A2 | ORG/PROJECT/{REPO} azure_active_directory     | ORG/REPO ado                 | FAIL
        A3 | ORG/PROJECT/{REPO} azure_active_directory     | ORG/GROUP/REPO ado           | OK
        A4 | ORG/PROJECT/{REPO} azure_active_directory     | ORG/GROUP/REPO               | FAIL
        B1 | ORG/PROJECT/{REPO} ado                        | ORG/REPO azure_devops        | OK
        B2 | ORG/PROJECT/{REPO} ado                        | ORG/REPO ado                 | FAIL
        B3 | ORG/PROJECT/{REPO} ado                        | ORG/GROUP/REPO ado           | OK
        B4 | ORG/PROJECT/{REPO} ado                        | ORG/GROUP/REPO azure_devops  | OK
    """

    def test_use_case_A1(self, git_repositories_aad: list[dict]) -> None:
        """A1: ORG/PROJECT/{REPO} azure_active_directory + ORG/REPO azure_devops"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/test",
            provider="azure_devops",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/test",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="other-org/test",
                provider="azure_devops",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/test",
                provider="github",
            )

    def test_use_case_A2(self, git_repositories_aad: list[dict]) -> None:
        """A2: ORG/PROJECT/{REPO} azure_active_directory + ORG/REPO ado"""

        """With provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/test",
                provider="ado",
            )

        """Without provider: OK (no provider => matches legacy like A1)"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/test",
        )
        assert resolved == "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"

    def test_use_case_A3(self, git_repositories_aad: list[dict]) -> None:
        """A3: ORG/PROJECT/{REPO} azure_active_directory + ORG/GROUP/REPO ado"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/aad-project/test",
            provider="ado",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK (logs warning but allows unintended usage for backward compatibility)"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/aad-project/test",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="other-org/aad-project/test",
                provider="ado",
            )

        """Another group: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/other-group/test",
                provider="ado",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/aad-project/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_aad,
                package_name="aad-labs/aad-project/test",
                provider="github",
            )

    def test_use_case_A4(self, git_repositories_aad: list[dict]) -> None:
        """A4: ORG/PROJECT/{REPO} azure_active_directory + ORG/GROUP/REPO (no provider)"""

        """With provider: OK (logs warning but allows unintended usage for backward compatibility)"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/aad-project/test",
            provider="azure_devops",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK (logs warning but allows unintended usage for backward compatibility)"""
        resolved = self.resolve(
            git_providers=git_repositories_aad,
            package_name="aad-labs/aad-project/test",
        )
        expected = "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        assert resolved == expected

    def test_use_case_B1(self, git_repositories_ado_with_legacy: list[dict]) -> None:
        """B1: ORG/PROJECT/{REPO} ado + ORG/REPO azure_devops"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado_with_legacy,
            package_name="ado-labs/test",
            provider="azure_devops",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK (first match wins - legacy config)"""
        resolved = self.resolve(
            git_providers=git_repositories_ado_with_legacy,
            package_name="ado-labs/test",
        )
        assert resolved == "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_with_legacy,
                package_name="other-org/test",
                provider="azure_devops",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_with_legacy,
                package_name="ado-labs/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_with_legacy,
                package_name="ado-labs/test",
                provider="github",
            )

    def test_use_case_B2(self, git_repositories_ado: list[dict]) -> None:
        """B2: ORG/PROJECT/{REPO} ado + ORG/REPO ado"""

        """With provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/test",
                provider="ado",
            )

        """Without provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/test",
            )

    def test_use_case_B3(self, git_repositories_ado: list[dict]) -> None:
        """B3: ORG/PROJECT/{REPO} ado + ORG/GROUP/REPO ado"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado,
            package_name="ado-labs/ado-project/test",
            provider="ado",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado,
            package_name="ado-labs/ado-project/test",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="other-org/ado-project/test",
                provider="ado",
            )

        """Another group: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/other-group/test",
                provider="ado",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/ado-project/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/ado-project/test",
                provider="github",
            )

    def test_use_case_B4(self, git_repositories_ado: list[dict]) -> None:
        """B4: ORG/PROJECT/{REPO} ado + ORG/GROUP/REPO azure_devops"""

        """With provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado,
            package_name="ado-labs/ado-project/test",
            provider="azure_devops",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado,
            package_name="ado-labs/ado-project/test",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Another org: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="other-org/ado-project/test",
                provider="azure_devops",
            )

        """Another group: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/other-group/test",
                provider="azure_devops",
            )

        """Gitlab: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/ado-project/test",
                provider="gitlab",
            )

        """Github: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado,
                package_name="ado-labs/ado-project/test",
                provider="github",
            )


# -----------------------------------------------------------------------------
# ADO Wildcard {project} URL Use Cases (org-level dedup, new format from dbt-cloud)
# -----------------------------------------------------------------------------
class TestADOWildcardProjectResolution(BasePrivatePackageHelperResolution):
    """
    New org-level ADO format where dbt-cloud emits one entry per org with {project} wildcard.

    dbt-cloud format (new):
    - ORG/{project}/_git/{repo} ado

    Expected use cases:

        ID | dbt-cloud                             | core (packages.yml)           | Expected
        ---|---------------------------------------|-------------------------------|---------
        C1 | ORG/{project}/_git/{repo} ado         | ORG/PROJECT/REPO ado          | OK — fills {project} from group
        C2 | ORG/{project}/_git/{repo} ado         | ORG/PROJECT/REPO azure_devops | OK — providers equivalent
        C3 | ORG/{project}/_git/{repo} ado         | ORG/REPO azure_devops         | FAIL — no project to fill wildcard
        C4 | ORG/{project}/_git/{repo} ado         | ORG/REPO ado                  | FAIL — 2-part ado always invalid
        C5 | ORG/{project}/_git/{repo} ado         | OTHER-ORG/PROJECT/REPO ado    | FAIL — org mismatch
        C6 | ORG/{project}/_git/{repo} ado         | ORG/PROJECT-A/REPO ado        | OK — any project fills the wildcard
        C7 | ORG/{project}/_git/{repo} ado         | ORG/PROJECT-B/REPO ado        | OK — different project, different URL
    """

    def test_use_case_C1(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C1: ORG/{project}/_git/{repo} ado + ORG/PROJECT/REPO ado"""

        """With provider: OK — {project} filled from group"""
        resolved = self.resolve(
            git_providers=git_repositories_ado_wildcard,
            package_name="ado-labs/ado-project/test",
            provider="ado",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

        """Without provider: OK"""
        resolved = self.resolve(
            git_providers=git_repositories_ado_wildcard,
            package_name="ado-labs/ado-project/test",
        )
        assert resolved == expected

    def test_use_case_C2(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C2: ORG/{project}/_git/{repo} ado + ORG/PROJECT/REPO azure_devops"""

        """With provider: OK — ado and azure_devops are equivalent"""
        resolved = self.resolve(
            git_providers=git_repositories_ado_wildcard,
            package_name="ado-labs/ado-project/test",
            provider="azure_devops",
        )
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/test.git"
        assert resolved == expected

    def test_use_case_C3(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C3: ORG/{project}/_git/{repo} ado + ORG/REPO azure_devops — FAIL (no project)"""

        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_wildcard,
                package_name="ado-labs/test",
                provider="azure_devops",
            )

        """Without provider: FAIL"""
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_wildcard,
                package_name="ado-labs/test",
            )

    def test_use_case_C4(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C4: ORG/{project}/_git/{repo} ado + ORG/REPO ado — FAIL (2-part ado always invalid)"""

        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_wildcard,
                package_name="ado-labs/test",
                provider="ado",
            )

    def test_use_case_C5(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C5: ORG/{project}/_git/{repo} ado + OTHER-ORG/PROJECT/REPO ado — FAIL (org mismatch)"""

        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_ado_wildcard,
                package_name="other-org/ado-project/test",
                provider="ado",
            )

    def test_use_case_C6(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C6: wildcard matches any project name — fills {project} from package definition"""

        resolved_a = self.resolve(
            git_providers=git_repositories_ado_wildcard,
            package_name="ado-labs/project-alpha/repo-x",
            provider="ado",
        )
        assert (
            resolved_a == "https://ado_token@dev.azure.com/ado-labs/project-alpha/_git/repo-x.git"
        )

    def test_use_case_C7(self, git_repositories_ado_wildcard: list[dict]) -> None:
        """C7: different project produces different resolved URL"""

        resolved_b = self.resolve(
            git_providers=git_repositories_ado_wildcard,
            package_name="ado-labs/project-beta/repo-y",
            provider="ado",
        )
        assert (
            resolved_b == "https://ado_token@dev.azure.com/ado-labs/project-beta/_git/repo-y.git"
        )


# -----------------------------------------------------------------------------
# MULTI-PROVIDER Private Package Use Cases
# -----------------------------------------------------------------------------
class TestPrivatePackageHelperMultiProvider(BasePrivatePackageHelperResolution):
    """Multi-provider resolution: GitHub, ADO, GitLab configured together."""

    def test_resolves_github_repo(self, git_repositories_multi_provider: list[dict]):
        expected = "https://github_token@github.com/github-labs/{repo}.git"
        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="github-labs/test",
            provider="github",
        )
        assert resolved == expected.format(repo="test")

        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="github-labs/test",
        )
        assert resolved == expected.format(repo="test")

    def test_resolves_ado_repo(self, git_repositories_multi_provider: list[dict]):
        """Legacy ADO: azure_active_directory / azure_devops with 2-part format."""
        expected = "https://ado_token@dev.azure.com/ado-labs/ado-project/_git/{repo}.git"
        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="ado-labs/test",
            provider="azure_active_directory",
        )
        assert resolved == expected.format(repo="test")

        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="ado-labs/test",
            provider="azure_devops",
        )
        assert resolved == expected.format(repo="test")

        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="ado-labs/test",
        )
        assert resolved == expected.format(repo="test")

    def test_resolves_ado_vs_legacy(self, git_repositories_multi_provider: list[dict]):
        """provider=ado with org/project/repo matches ADO; provider=azure_active_directory with org/repo matches legacy (git_repositories_aad)."""
        resolved_ado = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="ado-labs/other-project/test",
            provider="ado",
        )
        assert "ado_token_2" in resolved_ado
        assert "other-project" in resolved_ado
        assert (
            resolved_ado
            == "https://ado_token_2@dev.azure.com/ado-labs/other-project/_git/test.git"
        )

        resolved_legacy = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="aad-labs/test",
            provider="azure_active_directory",
        )
        assert "aad_token" in resolved_legacy
        assert "aad-project" in resolved_legacy
        assert (
            resolved_legacy == "https://aad_token@dev.azure.com/aad-labs/aad-project/_git/test.git"
        )

    def test_resolves_gitlab_repo_with_subgroup(self, git_repositories_multi_provider: list[dict]):
        """GitLab N-part: org/gitlab-project/test matches gitlab_n_part config."""
        expected = "https://gitlab_group_token@gitlab.com/gitlab-labs/gitlab-project/test.git"
        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="gitlab-labs/gitlab-project/test",
            provider="gitlab",
        )
        assert resolved == expected

        resolved = self.resolve(
            git_providers=git_repositories_multi_provider,
            package_name="gitlab-labs/gitlab-project/test",
        )
        assert resolved == expected

    def test_rejects_wrong_provider_for_repo(self, git_repositories_multi_provider: list[dict]):
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_multi_provider,
                package_name="github-labs/test",
                provider="gitlab",
            )
        with pytest.raises(PrivatePackageResolutionError):
            self.resolve(
                git_providers=git_repositories_multi_provider,
                package_name="gitlab-labs/gitlab-project/test",
                provider="github",
            )

