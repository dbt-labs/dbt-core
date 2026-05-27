"""End-to-end tests for the --use-v2-parser branch.

We don't depend on the real `fs` binary. Instead, we run dbt-core's own
parser once to produce a real manifest.json, stash it, then inject a fake
"fs" script that copies the stash into target/manifest.json on demand.
That gives us a known-good fusion-shaped artifact and lets us assert
that dbt-core's load + dispatch logic round-trips through it correctly.
"""

import shlex
import shutil
import stat
from pathlib import Path
from unittest import mock

import pytest

from dbt.tests.util import run_dbt

MODEL_A_SQL = """
select 1 as id
"""

MODEL_B_SQL = """
select * from {{ ref('model_a') }}
"""

SCHEMA_YML = """
version: 2
models:
  - name: model_a
  - name: model_b
"""


class FusionParserFixture:
    @pytest.fixture(autouse=True)
    def _stub_plugin_enrichment(self):
        """Mantle registers global plugins (dbtCloudAutoExposures,
        dbtCloudCrossProjectRef) that advertise `get_nodes`, which the fusion
        branch refuses by design. These tests exercise the fusion code path
        itself, not plugin interop, so stub both the fail-fast check and the
        artifact-enrichment hook out."""
        with mock.patch("dbt.parser.manifest.assert_no_get_nodes_plugins"), mock.patch(
            "dbt.parser.manifest.enrich_manifest_with_plugin_artifacts"
        ):
            yield

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": MODEL_A_SQL,
            "model_b.sql": MODEL_B_SQL,
            "schema.yml": SCHEMA_YML,
        }

    @pytest.fixture(scope="class")
    def fake_fs(self, project, tmp_path_factory):
        """Seed a real manifest.json via core's parser, then build a shell
        script that copies it into <target>/manifest.json on invocation."""
        run_dbt(["parse"])
        seed_path = Path(project.project_root) / "target" / "manifest.json"
        assert seed_path.exists(), "core parse failed to produce manifest.json"

        stash_dir = tmp_path_factory.mktemp("fusion_seed")
        stash = stash_dir / "manifest.json"
        shutil.copy(seed_path, stash)

        # Wipe the real manifest so we can prove the fake produced it.
        seed_path.unlink()

        bin_dir = tmp_path_factory.mktemp("fusion_bin")
        fake = bin_dir / "fake_fs.sh"
        fake.write_text(
            f"""#!/usr/bin/env bash
# Tiny stand-in for `fs parse`. Writes the stashed manifest.json into
# whatever --target-path argument we receive (defaults to ./target).
set -euo pipefail
TARGET_PATH="target"
while [ $# -gt 0 ]; do
  case "$1" in
    --target-path)
      TARGET_PATH="$2"
      shift 2
      ;;
    --project-dir)
      cd "$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "$TARGET_PATH"
cp {shlex.quote(str(stash))} "$TARGET_PATH/manifest.json"
"""
        )
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return fake


class TestFusionParserBranch(FusionParserFixture):
    def test_fusion_branch_loads_manifest(self, project, fake_fs):
        results = run_dbt(
            [
                "--use-v2-parser",
                f"--v2-parser={fake_fs}",
                "parse",
            ]
        )
        # parse returns the manifest object
        assert results is not None
        assert "model.test.model_a" in results.nodes
        assert "model.test.model_b" in results.nodes

    def test_fusion_branch_deletes_stale_partial_parse(self, project, fake_fs):
        target = Path(project.project_root) / "target"
        target.mkdir(exist_ok=True)
        stale = target / "partial_parse.msgpack"
        stale.write_bytes(b"stale-cache")
        run_dbt(
            [
                "--use-v2-parser",
                f"--v2-parser={fake_fs}",
                "parse",
            ]
        )
        assert not stale.exists(), "stale partial_parse.msgpack should be deleted"

    def test_missing_fs_binary_raises(self, project):
        with pytest.raises(Exception, match="(?i)fusion parser|not found"):
            run_dbt(
                [
                    "--use-v2-parser",
                    "--v2-parser=definitely-not-a-real-binary-xyz",
                    "parse",
                ],
                expect_pass=False,
            )

    def test_default_path_unchanged(self, project):
        """With the flag off, parse goes through the regular pipeline."""
        results = run_dbt(["parse"])
        assert results is not None
        assert "model.test.model_a" in results.nodes
