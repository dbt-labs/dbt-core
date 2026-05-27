"""Fusion parser integration.

Delegates parsing to an external `fs parse` subprocess that produces a
manifest.json on disk. dbt-core then loads that manifest and converts it
to a runtime Manifest, bypassing its own parser entirely.

See docs/arch/fusion_parser_design.md for the full design and rollout plan.
"""

from __future__ import annotations

import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from dbt.artifacts.exceptions import IncompatibleSchemaError
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import (
    FusionParserError,
    FusionParserMissingError,
    FusionParserSchemaError,
    FusionParserVersionError,
)

if TYPE_CHECKING:
    from dbt.cli.flags import Flags
    from dbt.config import RuntimeConfig


def parse_with_fusion(flags: "Flags", runtime_config: "RuntimeConfig") -> Manifest:
    """Invoke fs parse, load the resulting manifest.json, return runtime Manifest.

    fs is run into a temp handoff dir rather than the project's target dir so
    that (a) we can detect "fs exited 0 without writing" instead of silently
    loading a stale manifest from a prior run, and (b) `--no-write-json`
    doesn't leak a manifest.json into the user's target dir.
    """
    project_target_path = Path(runtime_config.project_target_path)

    with tempfile.TemporaryDirectory(prefix="dbt-fusion-") as handoff_dir:
        handoff = Path(handoff_dir)
        argv = _build_argv(flags, target_path_override=str(handoff))

        _run_fusion(argv)

        manifest_path = handoff / "manifest.json"
        if not manifest_path.exists():
            raise FusionParserError(
                f"fs parse exited successfully but did not produce {manifest_path.name} "
                f"in the handoff directory."
            )

        writable = _load_writable_manifest(manifest_path)

        if getattr(flags, "WRITE_JSON", False):
            project_target_path.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(manifest_path, project_target_path / "manifest.json")

    manifest = Manifest.from_writable_manifest(writable)
    # build_flat_graph is normally called by ManifestLoader.get_full_manifest;
    # the fusion path bypasses that loader, so populate flat_graph here to
    # power the `graph` context variable (graph.nodes, graph.sources, ...).
    manifest.build_flat_graph()

    _delete_stale_partial_parse(project_target_path)

    return manifest


def _build_argv(flags: "Flags", target_path_override: Optional[str] = None) -> List[str]:
    """Translate dbt-core flags into fs CLI args.

    The base command is taken from flags.V2_PARSER_COMMAND (default 'fs parse')
    and split with shlex so users can configure subcommands or wrappers.

    Forwarded flags (must affect manifest output):
      --project-dir, --profiles-dir, --profile, --target,
      --target-path, --vars, --packages-install-path

    When target_path_override is provided, it replaces the user's --target-path
    so fs writes its handoff manifest where dbt expects it (a temp dir).
    """
    base = shlex.split(getattr(flags, "V2_PARSER_COMMAND", "fs parse"))
    forwarded: List[str] = []

    project_dir = getattr(flags, "PROJECT_DIR", None)
    if project_dir:
        forwarded += ["--project-dir", str(project_dir)]

    profiles_dir = getattr(flags, "PROFILES_DIR", None)
    if profiles_dir:
        forwarded += ["--profiles-dir", str(profiles_dir)]

    profile = getattr(flags, "PROFILE", None)
    if profile:
        forwarded += ["--profile", profile]

    target = getattr(flags, "TARGET", None)
    if target:
        forwarded += ["--target", target]

    if target_path_override is not None:
        forwarded += ["--target-path", target_path_override]
    else:
        target_path = getattr(flags, "TARGET_PATH", None)
        if target_path:
            forwarded += ["--target-path", str(target_path)]

    packages_install_path = getattr(flags, "PACKAGES_INSTALL_PATH", None)
    if packages_install_path:
        forwarded += ["--packages-install-path", str(packages_install_path)]

    cli_vars = getattr(flags, "VARS", None)
    if cli_vars:
        forwarded += ["--vars", _serialize_vars(cli_vars)]

    return base + forwarded


def _run_fusion(argv: List[str]) -> None:
    # Passthrough mode: fs inherits dbt's stdout/stderr so users see progress
    # and errors live. This bypasses dbt's event system (no log-file capture,
    # no --log-format json, no level filtering), but is the only way to get
    # streaming output without re-parsing fs's free-form text.
    #
    # TODO: replace with Popen + line-by-line streaming and re-emit through
    # fire_event once fs ships its structured (JSON) log stream and the
    # Python parsing library lands. Sketch:
    #   proc = subprocess.Popen(argv, stdout=PIPE, stderr=PIPE, text=True, bufsize=1)
    #   - read stdout/stderr concurrently (threads or selectors; a single
    #     blocking readline() on one stream deadlocks if the other fills its pipe)
    #   - parse each line as a fs structured log record
    #   - map level/message to a dbt event type and fire_event(...)
    #   - proc.wait() and check returncode
    # Until then, capturing-then-printing-at-end would lose streaming (fs parse
    # can take minutes on large projects), so we inherit fds instead.
    try:
        result = subprocess.run(argv, check=False)
    except FileNotFoundError as e:
        raise FusionParserMissingError(
            f"Fusion parser command not found: {argv[0]!r}. "
            f"Ensure 'fs' is installed and on PATH, or set --v2-parser-command."
        ) from e

    if result.returncode != 0:
        raise FusionParserError(
            f"Fusion parser failed (exit {result.returncode}); see fs output above."
        )


def _load_writable_manifest(path: Path) -> WritableManifest:
    try:
        return WritableManifest.read_and_check_versions(str(path))
    except IncompatibleSchemaError as e:
        raise FusionParserVersionError(
            f"Fusion-produced manifest at {path} has an incompatible schema "
            f"version: expected {e.expected}, found {e.found}."
        ) from e
    except Exception as e:
        raise FusionParserSchemaError(
            f"Could not load fusion-produced manifest at {path}: {e}"
        ) from e


def _serialize_vars(cli_vars) -> str:
    """Serialize the resolved --vars dict to a YAML string for fs.

    dbt-core's --vars is parsed into a dict by click via the YAML param type
    (cli/params.py vars). Forward as a compact YAML string so fs receives a
    single canonical value rather than re-resolving env vars or layered configs.
    """
    import yaml

    if isinstance(cli_vars, str):
        return cli_vars
    return yaml.safe_dump(cli_vars, default_flow_style=True).strip()


def _delete_stale_partial_parse(target_path: Path) -> None:
    """Remove partial_parse.msgpack written by a prior non-fusion run.

    The msgpack cache is owned by dbt-core's parser; in fusion mode it is no
    longer written, and a later non-fusion run would load a cache whose
    file_id mappings predate any fusion-era source changes. Deleting on
    fusion entry is harmless if absent and unambiguous if present.
    """
    msgpack = target_path / "partial_parse.msgpack"
    if msgpack.exists():
        msgpack.unlink()
