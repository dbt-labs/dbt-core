import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest

from dbt.exceptions import (
    FusionParserError,
    FusionParserMissingError,
    FusionParserSchemaError,
    FusionParserVersionError,
)
from dbt.parser.fusion import (
    _build_argv,
    _delete_stale_partial_parse,
    _serialize_vars,
    parse_with_fusion,
)


def _flags(**overrides):
    base = {
        "V2_PARSER": "fs parse",
        "PROJECT_DIR": None,
        "PROFILES_DIR": None,
        "PROFILE": None,
        "TARGET": None,
        "TARGET_PATH": None,
        "PACKAGES_INSTALL_PATH": None,
        "VARS": None,
        "WRITE_JSON": True,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _fake_fs(manifest_text: Optional[str], returncode: int = 0, stderr: str = ""):
    """Build a subprocess.run side_effect that writes manifest.json into the
    --target-path argv slot. Pass manifest_text=None to simulate an fs run
    that exits successfully without writing a manifest."""

    def _run(argv, *args, **kwargs):
        if manifest_text is not None and returncode == 0:
            target_path = Path(argv[argv.index("--target-path") + 1])
            target_path.mkdir(parents=True, exist_ok=True)
            (target_path / "manifest.json").write_text(manifest_text)
        return subprocess.CompletedProcess(
            args=argv, returncode=returncode, stdout="", stderr=stderr
        )

    return _run


@pytest.fixture(autouse=True)
def _no_invocation_id():
    """Default to no invocation id so argv tests assert only the flags they set."""
    with mock.patch("dbt.parser.fusion.get_invocation_id", return_value=None):
        yield


class TestBuildArgv:
    def test_default_command_no_forwards(self):
        assert _build_argv(_flags()) == ["fs", "parse"]

    def test_forwards_all_known_flags(self):
        argv = _build_argv(
            _flags(
                PROJECT_DIR="/proj",
                PROFILES_DIR="/profiles",
                PROFILE="my_profile",
                TARGET="dev",
                TARGET_PATH="target",
                PACKAGES_INSTALL_PATH="dbt_packages",
                VARS={"k": "v"},
            )
        )
        assert argv[:2] == ["fs", "parse"]
        for pair in [
            ("--project-dir", "/proj"),
            ("--profiles-dir", "/profiles"),
            ("--profile", "my_profile"),
            ("--target", "dev"),
            ("--target-path", "target"),
            ("--packages-install-path", "dbt_packages"),
        ]:
            i = argv.index(pair[0])
            assert argv[i + 1] == pair[1]
        i = argv.index("--vars")
        assert "k" in argv[i + 1] and "v" in argv[i + 1]

    def test_custom_command_split_with_shlex(self):
        argv = _build_argv(_flags(V2_PARSER="uv run fs parse"))
        assert argv == ["uv", "run", "fs", "parse"]

    def test_target_path_override_replaces_user_value(self):
        argv = _build_argv(_flags(TARGET_PATH="user/target"), target_path_override="/tmp/handoff")
        i = argv.index("--target-path")
        assert argv[i + 1] == "/tmp/handoff"
        assert "user/target" not in argv

    def test_forwards_invocation_id(self):
        with mock.patch(
            "dbt.parser.fusion.get_invocation_id",
            return_value="11111111-1111-1111-1111-111111111111",
        ):
            argv = _build_argv(_flags())
        i = argv.index("--invocation-id")
        assert argv[i + 1] == "11111111-1111-1111-1111-111111111111"

    def test_omits_invocation_id_when_unavailable(self):
        argv = _build_argv(_flags())
        assert "--invocation-id" not in argv


class TestSerializeVars:
    def test_dict_to_yaml(self):
        out = _serialize_vars({"a": 1, "b": "two"})
        assert "a:" in out and "b:" in out

    def test_passthrough_string(self):
        assert _serialize_vars("a: 1") == "a: 1"


class TestDeleteStalePartialParse:
    def test_deletes_when_present(self, tmp_path: Path):
        msgpack = tmp_path / "partial_parse.msgpack"
        msgpack.write_bytes(b"stale")
        _delete_stale_partial_parse(tmp_path)
        assert not msgpack.exists()

    def test_noop_when_absent(self, tmp_path: Path):
        _delete_stale_partial_parse(tmp_path)


@pytest.fixture
def _patch_fusion_deps():
    """parse_with_fusion now resolves flags via get_flags() and calls
    assert_no_get_nodes_plugins / enrich_manifest_with_plugin_artifacts on the
    real plugin manager. Stub them so tests focus on fs invocation behavior.
    """
    with mock.patch("dbt.parser.fusion.get_flags", return_value=_flags()), mock.patch(
        "dbt.parser.manifest.assert_no_get_nodes_plugins"
    ), mock.patch("dbt.parser.manifest.enrich_manifest_with_plugin_artifacts"):
        yield


class TestParseWithFusion:
    def _runtime_config(self, target_path: Path):
        return SimpleNamespace(project_target_path=str(target_path), project_name="test")

    def test_missing_binary_raises_typed_error(self, tmp_path: Path, _patch_fusion_deps):
        with mock.patch(
            "dbt.parser.fusion.get_flags",
            return_value=_flags(V2_PARSER="definitely-not-a-real-binary-xyz"),
        ), mock.patch("dbt.parser.fusion.subprocess.run", side_effect=FileNotFoundError()):
            with pytest.raises(FusionParserMissingError):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_sets_dbt_invocation_env_on_subprocess(self, tmp_path: Path, _patch_fusion_deps):
        """fs must see DBT_INVOCATION_ENV=dbt-core-v2-parser regardless of the
        parent process's value, so analytics can attribute the embedded fs run
        to the v2-parser pathway without clobbering the host's own telemetry."""
        captured = {}

        def _capture(argv, *args, **kwargs):
            captured["env"] = kwargs.get("env")
            return _fake_fs(json.dumps({"metadata": {}}))(argv, *args, **kwargs)

        with mock.patch.dict(
            "os.environ", {"DBT_INVOCATION_ENV": "dbt-cloud-prod__host:cloud"}, clear=False
        ), mock.patch(
            "dbt.parser.fusion.subprocess.run", side_effect=_capture
        ), mock.patch(
            "dbt.parser.fusion._load_writable_manifest", return_value=mock.MagicMock()
        ), mock.patch(
            "dbt.parser.fusion.Manifest.from_writable_manifest", return_value=mock.MagicMock()
        ):
            parse_with_fusion(self._runtime_config(tmp_path), write=False, write_json=False)
            # parent process env is untouched (asserted inside the patch.dict
            # block so the original value is still in place to compare against)
            assert os.environ["DBT_INVOCATION_ENV"] == "dbt-cloud-prod__host:cloud"

        assert captured["env"] is not None
        assert captured["env"]["DBT_INVOCATION_ENV"] == "dbt-core-v2-parser"

    def test_nonzero_exit_raises(self, tmp_path: Path, _patch_fusion_deps):
        # Passthrough mode: fs's stderr streams directly to the user, so the
        # exception only carries the exit code, not the captured stderr.
        with mock.patch(
            "dbt.parser.fusion.subprocess.run",
            side_effect=_fake_fs(manifest_text=None, returncode=2),
        ):
            with pytest.raises(FusionParserError, match="exit 2"):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_missing_manifest_after_success_raises(self, tmp_path: Path, _patch_fusion_deps):
        """fs exits 0 but writes nothing — must raise, not silently load a stale file."""
        with mock.patch(
            "dbt.parser.fusion.subprocess.run", side_effect=_fake_fs(manifest_text=None)
        ):
            with pytest.raises(FusionParserError, match="did not produce"):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_stale_target_manifest_not_loaded(self, tmp_path: Path, _patch_fusion_deps):
        """A stale manifest left in target/ from a prior run must not satisfy
        the fusion handoff — fs writes into a fresh temp dir."""
        (tmp_path / "manifest.json").write_text(json.dumps({"stale": True}))
        with mock.patch(
            "dbt.parser.fusion.subprocess.run", side_effect=_fake_fs(manifest_text=None)
        ):
            with pytest.raises(FusionParserError, match="did not produce"):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_invalid_json_raises_schema_error(self, tmp_path: Path, _patch_fusion_deps):
        with mock.patch(
            "dbt.parser.fusion.subprocess.run", side_effect=_fake_fs("{ not valid json")
        ):
            with pytest.raises(FusionParserSchemaError):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_incompatible_schema_version_raises_version_error(
        self, tmp_path: Path, _patch_fusion_deps
    ):
        bad_version = json.dumps(
            {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v1.json"}}
        )
        with mock.patch("dbt.parser.fusion.subprocess.run", side_effect=_fake_fs(bad_version)):
            with pytest.raises(FusionParserVersionError):
                parse_with_fusion(self._runtime_config(tmp_path), write=True, write_json=True)

    def test_no_write_json_leaves_target_dir_untouched(self, tmp_path: Path, _patch_fusion_deps):
        """With write_json=False, the fusion handoff manifest must not be
        copied into the user's target dir."""
        # Pre-create target dir but leave it empty.
        target = tmp_path / "target"
        target.mkdir()
        with mock.patch(
            "dbt.parser.fusion.subprocess.run",
            side_effect=_fake_fs(json.dumps({"metadata": {}})),
        ), mock.patch(
            "dbt.parser.fusion._load_writable_manifest",
            return_value=mock.MagicMock(),
        ), mock.patch(
            "dbt.parser.fusion.Manifest.from_writable_manifest",
            return_value=mock.MagicMock(),
        ):
            parse_with_fusion(self._runtime_config(target), write=True, write_json=False)
        assert list(target.iterdir()) == []

    def test_write_json_copies_manifest_to_target_dir(self, tmp_path: Path, _patch_fusion_deps):
        target = tmp_path / "target"
        target.mkdir()
        with mock.patch(
            "dbt.parser.fusion.subprocess.run",
            side_effect=_fake_fs(json.dumps({"metadata": {}})),
        ), mock.patch(
            "dbt.parser.fusion._load_writable_manifest",
            return_value=mock.MagicMock(),
        ), mock.patch(
            "dbt.parser.fusion.Manifest.from_writable_manifest",
            return_value=mock.MagicMock(),
        ):
            parse_with_fusion(self._runtime_config(target), write=True, write_json=True)
        assert (target / "manifest.json").exists()
