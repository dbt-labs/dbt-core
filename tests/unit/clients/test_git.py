"""Unit tests for dbt.clients.git.

Regression coverage for https://github.com/dbt-labs/dbt-core/issues/10381:

    When a user configures git with columnar output (``git config column.ui
    always``), ``git tag --list`` prints tags in multiple columns:

        $ git -c column.ui=always tag --list
        v1.0.0  v1.1.0  v2.0.0  v2.2.0
        v1.0.1  v1.2.0  v2.1.0  v3.0.0

    ``list_tags`` splits stdout on newlines, so the columnar output collapses
    many tags into a few whitespace-mangled strings, and ``dbt deps`` can no
    longer match a requested revision against the tag list. ``git tag
    --no-column`` forces one-tag-per-line regardless of the user's git config,
    which is what these tests lock in.
"""

import shutil
import subprocess
from unittest import mock

import pytest

from dbt.clients import git


class TestListTagsCommand:
    """The command contract: list_tags must call ``git tag --no-column``."""

    def test_uses_no_column_flag(self):
        with mock.patch.object(git, "run_cmd", return_value=(b"v1.0.0\n", b"")) as run_cmd:
            git.list_tags("/some/repo")

        run_cmd.assert_called_once_with(
            "/some/repo", ["git", "tag", "--no-column"], env={"LC_ALL": "C"}
        )

    def test_parses_one_tag_per_line(self):
        stdout = b"v1.0.0\nv1.0.1\nv2.0.0\n"
        with mock.patch.object(git, "run_cmd", return_value=(stdout, b"")):
            tags = git.list_tags("/some/repo")

        assert tags == ["v1.0.0", "v1.0.1", "v2.0.0"]


@pytest.mark.skipif(shutil.which("git") is None, reason="git executable not available")
class TestListTagsAgainstRealGit:
    """End-to-end regression: real repo + columnar git config must still parse."""

    def _git(self, cwd, *args):
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)

    def _init_repo_with_tags(self, cwd, tags, columnar):
        self._git(cwd, "init")
        self._git(cwd, "config", "user.email", "test@example.com")
        self._git(cwd, "config", "user.name", "dbt test")
        self._git(cwd, "config", "commit.gpgsign", "false")
        if columnar:
            # The trigger for #10381: force columnar output even when stdout
            # is a pipe (as it is under subprocess).
            self._git(cwd, "config", "column.ui", "always")
        self._git(cwd, "commit", "--allow-empty", "-m", "init")
        for tag in tags:
            self._git(cwd, "tag", tag)

    def test_columnar_config_still_yields_clean_tags(self, tmp_path, monkeypatch):
        # Pin the column width so the columnar path is exercised deterministically.
        # Without this, a tiny COLUMNS in the runner's environment would make git
        # emit one tag per line regardless, letting this regression test pass
        # vacuously (it would still pass even if the fix were reverted to --list).
        monkeypatch.setenv("COLUMNS", "80")
        tags = [
            "v1.0.0",
            "v1.0.1",
            "v1.1.0",
            "v1.2.0",
            "v2.0.0",
            "v2.1.0",
            "v2.2.0",
            "v3.0.0",
            "v3.1.0",
            "v3.2.0",
        ]
        cwd = str(tmp_path)
        self._init_repo_with_tags(cwd, tags, columnar=True)

        # Precondition: confirm columnar output actually collapses the tags onto
        # fewer physical lines than there are tags. That collapse is exactly what
        # breaks the old `git tag --list` newline parsing; if it does not happen
        # the assertion below would be meaningless, so fail loudly instead.
        raw = subprocess.run(
            ["git", "tag"], cwd=cwd, check=True, capture_output=True
        ).stdout.decode("utf-8")
        assert len(raw.strip().splitlines()) < len(
            tags
        ), "columnar output did not trigger; regression test would be vacuous"

        result = git.list_tags(cwd)

        # With --no-column every tag is its own clean element. Under --list the
        # columnar output above would collapse into a few mangled strings.
        assert len(result) == len(tags)
        assert sorted(result) == sorted(tags)

    def test_default_config_yields_clean_tags(self, tmp_path):
        tags = ["v1.0.0", "v1.1.0", "v2.0.0"]
        cwd = str(tmp_path)
        self._init_repo_with_tags(cwd, tags, columnar=False)

        assert sorted(git.list_tags(cwd)) == sorted(tags)

    def test_tagless_repo_returns_single_empty_string(self, tmp_path):
        # Documents pre-existing behavior this fix does NOT change: a repo with
        # no tags yields [""] (not []), because list_tags splits stripped stdout
        # on newlines. Captured so the contract is explicit; not an endorsement.
        cwd = str(tmp_path)
        self._init_repo_with_tags(cwd, tags=[], columnar=False)

        assert git.list_tags(cwd) == [""]
