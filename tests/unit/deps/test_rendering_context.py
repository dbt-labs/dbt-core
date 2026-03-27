"""Tests for rendering context tracking and change detection in DepsTask.

Verifies that:
- PackageRenderer tracks which vars, target keys, and env vars are accessed
- DepsTask._get_rendering_context() captures only referenced context
- DepsTask._rendering_context_changed() detects changes in referenced context
- Re-lock is triggered when referenced var/env_var/target values change
- Re-lock is NOT triggered when unrelated var/env_var/target values change

Scenarios covered for both root-level and transitive dependencies:
1. var from dbt_project.yml changes -> re-lock
2. var overridden via --vars CLI changes -> re-lock
3. env_var changes -> re-lock
4. target changes -> re-lock
5. Unrelated context changes -> no re-lock
"""

import os
import unittest
from argparse import Namespace
from unittest import mock

from dbt.config.renderer import PackageRenderer, _TrackingDict
from dbt.constants import PACKAGE_LOCK_CONTEXT_KEY
from dbt.flags import set_from_args
from dbt.task.deps import DepsTask
from dbt.tests.util import safe_set_invocation_context

set_from_args(Namespace(WARN_ERROR=False), None)


def _make_deps_task(project_vars=None, cli_vars=None, profile=None):
    """Create a DepsTask with mocked internals for unit testing."""
    if project_vars is None:
        project_vars = {}
    if cli_vars is None:
        cli_vars = {}

    mock_project = mock.MagicMock()
    mock_project.vars.to_dict.return_value = project_vars

    mock_args = Namespace(vars=cli_vars)

    with mock.patch("dbt.task.deps.BaseTask.__init__"):
        task = DepsTask.__new__(DepsTask)
        task.project = mock_project
        task.cli_vars = cli_vars
        task.profile = profile
        task.args = mock_args

    return task


def _make_renderer_with_accesses(
    cli_vars=None,
    target_dict=None,
    access_vars=None,
    access_target_keys=None,
    access_env_vars=None,
):
    """Create a PackageRenderer and simulate accesses to track context.

    Args:
        cli_vars: Dict of vars available to the renderer.
        target_dict: Dict of target values available.
        access_vars: List of var names to simulate accessing.
        access_target_keys: List of target keys to simulate accessing.
        access_env_vars: Dict of {name: value} to simulate env_var() accessing.
    """
    if cli_vars is None:
        cli_vars = {}

    # Set up invocation context with env vars before creating renderer
    if access_env_vars:
        for name, value in access_env_vars.items():
            if value is not None:
                os.environ[name] = value
    safe_set_invocation_context()

    renderer = PackageRenderer(cli_vars, target_dict=target_dict)

    # Simulate var() accesses — this triggers _TrackingDict tracking
    if access_vars:
        for var_name in access_vars:
            renderer.render_value("{{ var('" + var_name + "', '') }}")

    # Simulate target.key accesses
    if access_target_keys and target_dict:
        for key in access_target_keys:
            renderer.render_value("{{ target." + key + " }}")

    # Simulate env_var() accesses
    if access_env_vars:
        for name in access_env_vars:
            renderer.render_value("{{ env_var('" + name + "', '') }}")

    return renderer


def _build_lock_context(env_var_names=None, var_names=None, target_keys=None, context_hash=None):
    """Build a rendering_context dict as it would appear in package-lock.yml."""
    return {
        "env_var_names": env_var_names or [],
        "var_names": var_names or [],
        "target_keys": target_keys or [],
        "hash": context_hash or "",
    }


class TestTrackingDict(unittest.TestCase):
    """Tests for _TrackingDict key access tracking."""

    def test_getitem_tracks_key(self):
        d = _TrackingDict({"a": 1, "b": 2})
        _ = d["a"]
        self.assertEqual(d.accessed_keys, {"a"})

    def test_get_tracks_key(self):
        d = _TrackingDict({"a": 1})
        d.get("a")
        d.get("missing", "default")
        self.assertEqual(d.accessed_keys, {"a", "missing"})

    def test_contains_tracks_key(self):
        d = _TrackingDict({"a": 1})
        _ = "a" in d
        _ = "missing" in d
        self.assertEqual(d.accessed_keys, {"a", "missing"})

    def test_no_access_empty(self):
        d = _TrackingDict({"a": 1, "b": 2})
        self.assertEqual(d.accessed_keys, set())

    def test_multiple_accesses_same_key(self):
        d = _TrackingDict({"a": 1})
        _ = d["a"]
        _ = d["a"]
        d.get("a")
        self.assertEqual(d.accessed_keys, {"a"})


class TestPackageRendererTracking(unittest.TestCase):
    """Tests that PackageRenderer tracks var and target accesses."""

    def test_tracked_vars_records_var_access(self):
        safe_set_invocation_context()
        renderer = PackageRenderer(cli_vars={"my_var": "hello", "other_var": "world"})
        renderer.render_value("{{ var('my_var') }}")
        self.assertIn("my_var", renderer.tracked_vars.accessed_keys)
        self.assertNotIn("other_var", renderer.tracked_vars.accessed_keys)

    def test_tracked_vars_records_missing_var_with_default(self):
        safe_set_invocation_context()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ var('missing_var', 'fallback') }}")
        self.assertIn("missing_var", renderer.tracked_vars.accessed_keys)

    def test_tracked_target_records_key_access(self):
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics", "type": "postgres"}
        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        renderer.render_value("{{ target.name }}")
        self.assertIn("name", renderer.tracked_target.accessed_keys)
        self.assertNotIn("schema", renderer.tracked_target.accessed_keys)

    def test_no_target_tracking_when_no_target(self):
        safe_set_invocation_context()
        renderer = PackageRenderer(cli_vars={})
        self.assertIsNone(renderer.tracked_target)

    def test_env_var_tracked_by_context(self):
        os.environ["TEST_RENDER_ENV"] = "some_value"
        safe_set_invocation_context()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('TEST_RENDER_ENV') }}")
        self.assertIn("TEST_RENDER_ENV", renderer.ctx_obj.env_vars)
        os.environ.pop("TEST_RENDER_ENV", None)


# ---------------------------------------------------------------------------
# Root-level package context tracking tests
# ---------------------------------------------------------------------------


class TestRootLevelVarFromProject(unittest.TestCase):
    """Root pkg uses var() from dbt_project.yml. If var changes, re-lock."""

    def test_context_captures_accessed_var(self):
        """_get_rendering_context records the var that was accessed."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_pkg": True})
        renderer = PackageRenderer(cli_vars={"install_pkg": True})
        renderer.render_value("{{ var('install_pkg') }}")
        ctx = task._get_rendering_context(renderer)
        self.assertIn("install_pkg", ctx["var_names"])

    def test_var_change_triggers_relock(self):
        """When a referenced project var changes, _rendering_context_changed returns True."""
        safe_set_invocation_context()
        # First lock: var is True
        task = _make_deps_task(project_vars={"install_pkg": True})
        renderer = PackageRenderer(cli_vars={"install_pkg": True})
        renderer.render_value("{{ var('install_pkg') }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: var changed to False
        task2 = _make_deps_task(project_vars={"install_pkg": False})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_same_var_no_relock(self):
        """When a referenced project var stays the same, no re-lock."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_pkg": True})
        renderer = PackageRenderer(cli_vars={"install_pkg": True})
        renderer.render_value("{{ var('install_pkg') }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: same var value
        task2 = _make_deps_task(project_vars={"install_pkg": True})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_unrelated_var_change_no_relock(self):
        """When an unrelated var changes, no re-lock."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_pkg": True, "other": "a"})
        renderer = PackageRenderer(cli_vars={"install_pkg": True, "other": "a"})
        # Only install_pkg is accessed
        renderer.render_value("{{ var('install_pkg') }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertNotIn("other", lock_context["var_names"])

        # Second run: unrelated var changed
        task2 = _make_deps_task(project_vars={"install_pkg": True, "other": "b"})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestRootLevelVarFromCli(unittest.TestCase):
    """Root pkg uses var(). CLI --vars override triggers re-lock."""

    def test_cli_var_override_triggers_relock(self):
        """When a CLI var override changes, re-lock is triggered."""
        safe_set_invocation_context()
        # First lock: CLI sets install_pkg=True (overriding project default)
        task = _make_deps_task(project_vars={"install_pkg": False}, cli_vars={"install_pkg": True})
        # cli_vars override project_vars, so merged = {"install_pkg": True}
        merged = {}
        merged.update(task.project.vars.to_dict())
        merged.update(task.cli_vars)
        renderer = PackageRenderer(cli_vars=merged)
        renderer.render_value("{{ var('install_pkg') }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: CLI sets install_pkg=False
        task2 = _make_deps_task(
            project_vars={"install_pkg": False}, cli_vars={"install_pkg": False}
        )
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_cli_var_same_no_relock(self):
        """When CLI var stays the same, no re-lock."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={}, cli_vars={"install_pkg": True})
        renderer = PackageRenderer(cli_vars={"install_pkg": True})
        renderer.render_value("{{ var('install_pkg') }}")
        lock_context = task._get_rendering_context(renderer)

        task2 = _make_deps_task(project_vars={}, cli_vars={"install_pkg": True})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestRootLevelEnvVar(unittest.TestCase):
    """Root pkg uses env_var(). If env var changes, re-lock."""

    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        # Restore original environment
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_env_var_change_triggers_relock(self):
        """When a referenced env var changes, _rendering_context_changed returns True."""
        os.environ["ENABLE_PKG"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_PKG') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertIn("ENABLE_PKG", lock_context["env_var_names"])

        # Second run: env var changed
        os.environ["ENABLE_PKG"] = "false"
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_env_var_same_no_relock(self):
        """When a referenced env var stays the same, no re-lock."""
        os.environ["ENABLE_PKG"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_PKG') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: same env var value
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_env_var_removed_triggers_relock(self):
        """When a referenced env var is removed, re-lock is triggered."""
        os.environ["ENABLE_PKG"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_PKG') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: env var removed
        os.environ.pop("ENABLE_PKG", None)
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_unrelated_env_var_change_no_relock(self):
        """When an unrelated env var changes, no re-lock."""
        os.environ["ENABLE_PKG"] = "true"
        os.environ["UNRELATED"] = "a"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        # Only ENABLE_PKG is accessed
        renderer.render_value("{{ env_var('ENABLE_PKG') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertNotIn("UNRELATED", lock_context["env_var_names"])

        # Second run: unrelated env var changed
        os.environ["UNRELATED"] = "b"
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestRootLevelTarget(unittest.TestCase):
    """Root pkg uses target.name. If target changes, re-lock."""

    def test_target_change_triggers_relock(self):
        """When a referenced target key changes, re-lock is triggered."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertIn("name", lock_context["target_keys"])

        # Second run: target changed to prod
        new_target = {"name": "prod", "schema": "analytics"}
        mock_profile2 = mock.MagicMock()
        mock_profile2.to_target_dict.return_value = new_target
        task2 = _make_deps_task(profile=mock_profile2)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_target_same_no_relock(self):
        """When referenced target key stays the same, no re-lock."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: same target
        task2 = _make_deps_task(profile=mock_profile)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_unrelated_target_key_change_no_relock(self):
        """When an unrelated target key changes, no re-lock."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        # Only access target.name, not schema
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertNotIn("schema", lock_context["target_keys"])

        # Second run: schema changed (but name stayed)
        new_target = {"name": "dev", "schema": "new_schema"}
        mock_profile2 = mock.MagicMock()
        mock_profile2.to_target_dict.return_value = new_target
        task2 = _make_deps_task(profile=mock_profile2)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


# ---------------------------------------------------------------------------
# Transitive dependency context tracking tests
#
# Transitive deps are rendered by the same PackageRenderer passed to
# resolve_packages(). So tracked_vars/tracked_target/env_vars accumulate
# accesses from both root and transitive packages during resolution.
# These tests simulate that by making multiple render_value calls on
# the same renderer (as resolve_packages would during traversal).
# ---------------------------------------------------------------------------


class TestTransitiveVarFromProject(unittest.TestCase):
    """Transitive dep uses var() from dbt_project.yml. If var changes, re-lock."""

    def test_transitive_var_tracked(self):
        """Vars accessed by transitive deps are captured in rendering context."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_codegen": True, "root_var": "a"})
        renderer = PackageRenderer(cli_vars={"install_codegen": True, "root_var": "a"})
        # Root package accesses root_var
        renderer.render_value("{{ var('root_var') }}")
        # Transitive package accesses install_codegen
        renderer.render_value("{{ var('install_codegen') }}")

        ctx = task._get_rendering_context(renderer)
        self.assertIn("root_var", ctx["var_names"])
        self.assertIn("install_codegen", ctx["var_names"])

    def test_transitive_var_change_triggers_relock(self):
        """When a var used by transitive dep changes, re-lock is triggered."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_codegen": True})
        renderer = PackageRenderer(cli_vars={"install_codegen": True})
        # Transitive dep accesses install_codegen
        renderer.render_value("{{ var('install_codegen') }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: var changed
        task2 = _make_deps_task(project_vars={"install_codegen": False})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_transitive_var_same_no_relock(self):
        """When a var used by transitive dep stays the same, no re-lock."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_codegen": True})
        renderer = PackageRenderer(cli_vars={"install_codegen": True})
        renderer.render_value("{{ var('install_codegen') }}")
        lock_context = task._get_rendering_context(renderer)

        task2 = _make_deps_task(project_vars={"install_codegen": True})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_unrelated_var_change_no_relock_transitive(self):
        """Unrelated var change doesn't trigger re-lock even with transitive deps."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={"install_codegen": True, "unrelated": "x"})
        renderer = PackageRenderer(cli_vars={"install_codegen": True, "unrelated": "x"})
        renderer.render_value("{{ var('install_codegen') }}")
        lock_context = task._get_rendering_context(renderer)

        # Only install_codegen was tracked
        task2 = _make_deps_task(project_vars={"install_codegen": True, "unrelated": "y"})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestTransitiveVarFromCli(unittest.TestCase):
    """Transitive dep uses var(). CLI --vars override triggers re-lock."""

    def test_cli_var_override_for_transitive_dep(self):
        """CLI var override affecting transitive dep triggers re-lock."""
        safe_set_invocation_context()
        # First: CLI sets install_codegen=True
        task = _make_deps_task(project_vars={}, cli_vars={"install_codegen": True})
        renderer = PackageRenderer(cli_vars={"install_codegen": True})
        renderer.render_value("{{ var('install_codegen') }}")
        lock_context = task._get_rendering_context(renderer)

        # Second: CLI sets install_codegen=False
        task2 = _make_deps_task(project_vars={}, cli_vars={"install_codegen": False})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_cli_var_same_for_transitive_no_relock(self):
        """Same CLI var for transitive dep doesn't trigger re-lock."""
        safe_set_invocation_context()
        task = _make_deps_task(project_vars={}, cli_vars={"install_codegen": True})
        renderer = PackageRenderer(cli_vars={"install_codegen": True})
        renderer.render_value("{{ var('install_codegen') }}")
        lock_context = task._get_rendering_context(renderer)

        task2 = _make_deps_task(project_vars={}, cli_vars={"install_codegen": True})
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestTransitiveEnvVar(unittest.TestCase):
    """Transitive dep uses env_var(). If env var changes, re-lock."""

    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_transitive_env_var_change_triggers_relock(self):
        """Env var accessed by transitive dep triggers re-lock on change."""
        os.environ["ENABLE_CODEGEN"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        # Root accesses one env var, transitive accesses another
        renderer.render_value("{{ env_var('ENABLE_CODEGEN') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertIn("ENABLE_CODEGEN", lock_context["env_var_names"])

        # Second run: env var changed
        os.environ["ENABLE_CODEGEN"] = "false"
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_transitive_env_var_same_no_relock(self):
        """Same env var value for transitive dep doesn't trigger re-lock."""
        os.environ["ENABLE_CODEGEN"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_CODEGEN') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)

        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_transitive_env_var_removed_triggers_relock(self):
        """Removing an env var referenced by transitive dep triggers re-lock."""
        os.environ["ENABLE_CODEGEN"] = "true"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_CODEGEN') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: env var removed
        os.environ.pop("ENABLE_CODEGEN", None)
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_unrelated_env_var_change_no_relock_transitive(self):
        """Unrelated env var change doesn't trigger re-lock for transitive deps."""
        os.environ["ENABLE_CODEGEN"] = "true"
        os.environ["UNRELATED_ENV"] = "old"
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        renderer.render_value("{{ env_var('ENABLE_CODEGEN') == 'true' }}")
        lock_context = task._get_rendering_context(renderer)

        os.environ["UNRELATED_ENV"] = "new"
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


class TestTransitiveTarget(unittest.TestCase):
    """Transitive dep uses target. If target changes, re-lock."""

    def test_transitive_target_change_triggers_relock(self):
        """Target key accessed by transitive dep triggers re-lock on change."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        # Root uses target.name, transitive uses target.schema
        renderer.render_value("{{ target.name }}")
        renderer.render_value("{{ target.schema }}")
        lock_context = task._get_rendering_context(renderer)
        self.assertIn("name", lock_context["target_keys"])
        self.assertIn("schema", lock_context["target_keys"])

        # Second run: schema changed
        new_target = {"name": "dev", "schema": "new_schema"}
        mock_profile2 = mock.MagicMock()
        mock_profile2.to_target_dict.return_value = new_target
        task2 = _make_deps_task(profile=mock_profile2)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_transitive_target_same_no_relock(self):
        """Same target values for transitive dep doesn't trigger re-lock."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)

        task2 = _make_deps_task(profile=mock_profile)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))

    def test_unrelated_target_key_change_no_relock_transitive(self):
        """Unrelated target key change doesn't trigger re-lock for transitive deps."""
        safe_set_invocation_context()
        target = {"name": "dev", "schema": "analytics", "type": "postgres"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(profile=mock_profile)

        renderer = PackageRenderer(cli_vars={}, target_dict=target)
        # Only target.name is accessed
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)

        # Second run: type changed (but name stayed)
        new_target = {"name": "dev", "schema": "analytics", "type": "redshift"}
        mock_profile2 = mock.MagicMock()
        mock_profile2.to_target_dict.return_value = new_target
        task2 = _make_deps_task(profile=mock_profile2)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))


# ---------------------------------------------------------------------------
# Mixed context scenarios (multiple context types used together)
# ---------------------------------------------------------------------------


class TestMixedContextTracking(unittest.TestCase):
    """Tests with multiple context types used simultaneously."""

    def setUp(self):
        self._orig_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._orig_env)

    def test_mixed_var_and_env_var(self):
        """Both var and env_var are tracked; changing either triggers re-lock."""
        os.environ["MY_ENV"] = "val1"
        safe_set_invocation_context()
        target = {"name": "dev"}
        mock_profile = mock.MagicMock()
        mock_profile.to_target_dict.return_value = target
        task = _make_deps_task(project_vars={"my_var": "a"}, profile=mock_profile)
        renderer = PackageRenderer(cli_vars={"my_var": "a"}, target_dict=target)
        renderer.render_value("{{ var('my_var') }}")
        renderer.render_value("{{ env_var('MY_ENV') }}")
        renderer.render_value("{{ target.name }}")
        lock_context = task._get_rendering_context(renderer)

        # Verify all three types are tracked
        self.assertIn("my_var", lock_context["var_names"])
        self.assertIn("MY_ENV", lock_context["env_var_names"])
        self.assertIn("name", lock_context["target_keys"])

        # Change only env_var -> should re-lock
        os.environ["MY_ENV"] = "val2"
        safe_set_invocation_context()
        task2 = _make_deps_task(project_vars={"my_var": "a"}, profile=mock_profile)
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertTrue(task2._rendering_context_changed(lock_dict))

    def test_no_context_no_relock(self):
        """When no rendering context was stored, _rendering_context_changed returns False."""
        task = _make_deps_task()
        lock_dict = {}  # No PACKAGE_LOCK_CONTEXT_KEY
        self.assertFalse(task._rendering_context_changed(lock_dict))

    def test_empty_context_no_relock(self):
        """When rendering context has no tracked names, no re-lock even if env changes."""
        safe_set_invocation_context()
        task = _make_deps_task()
        renderer = PackageRenderer(cli_vars={})
        # No render_value calls — nothing tracked
        lock_context = task._get_rendering_context(renderer)

        os.environ["NEW_VAR"] = "something"
        safe_set_invocation_context()
        task2 = _make_deps_task()
        lock_dict = {PACKAGE_LOCK_CONTEXT_KEY: lock_context}
        self.assertFalse(task2._rendering_context_changed(lock_dict))
        os.environ.pop("NEW_VAR", None)
