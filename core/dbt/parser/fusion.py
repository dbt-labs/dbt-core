"""Fusion parser integration.

Delegates parsing to an external fusion parser subprocess that produces a
manifest.json on disk. dbt-core then loads that manifest and converts it
to a runtime Manifest, bypassing its own parser entirely.

This module implements the handoff to the fusion parser and loading of the
resulting manifest artifacts.
"""

from __future__ import annotations

import os
import platform
import shlex
import shutil
import subprocess
import sysconfig
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from dbt.artifacts.exceptions import IncompatibleSchemaError
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.contracts.graph.manifest import Manifest
from dbt.events.types import V2ParserEnd, V2ParserStart
from dbt.exceptions import (
    FusionParserError,
    FusionParserSchemaError,
    FusionParserVersionError,
)
from dbt.flags import get_flags
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event, get_invocation_id

if TYPE_CHECKING:
    from dbt.config import RuntimeConfig


def parse_with_fusion(
    runtime_config: "RuntimeConfig",
    write: bool,
    write_json: bool,
) -> Manifest:
    """Invoke the fusion parser, load the resulting manifest.json, return runtime Manifest.

    The fusion parser is run into a temp handoff dir rather than the project's
    target dir so that (a) we can detect "parser exited 0 without writing"
    instead of silently loading a stale manifest from a prior run, and (b)
    `--no-write-json` doesn't leak a manifest.json into the user's target dir.
    """
    from dbt.parser.manifest import (
        assert_no_get_nodes_plugins,
        enrich_manifest_with_plugin_artifacts,
    )

    assert_no_get_nodes_plugins(runtime_config.project_name)

    flags = get_flags()
    project_target_path = Path(runtime_config.project_target_path)
    v2_parser_command = getattr(flags, "V2_PARSER", "dbt-core-experimental-parser parse")
    project_name = runtime_config.project_name

    fire_event(V2ParserStart(v2_parser_command=v2_parser_command, project_name=project_name))
    start_time = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix="dbt-fusion-") as handoff_dir:
            handoff = Path(handoff_dir)
            argv = _build_argv(flags, target_path_override=str(handoff))

            _run_fusion(argv)

            manifest_path = handoff / "manifest.json"
            if not manifest_path.exists():
                raise FusionParserError(
                    f"Fusion parser exited successfully but did not produce {manifest_path.name} "
                    f"in the handoff directory."
                )

            writable_manifest = _load_writable_manifest(manifest_path)

            if write and write_json:
                # Copy v2 parser artifacts rather than re-serializing through write_manifest
                project_target_path.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(manifest_path, project_target_path / "manifest.json")
                semantic_manifest_path = handoff / "semantic_manifest.json"
                if semantic_manifest_path.exists():
                    shutil.copyfile(
                        semantic_manifest_path, project_target_path / "semantic_manifest.json"
                    )
    except (
        FusionParserVersionError,
        FusionParserSchemaError,
        FusionParserError,
    ) as e:
        fire_event(
            V2ParserEnd(
                status="failure",
                execution_time=time.monotonic() - start_time,
                error_class=type(e).__name__,
                exit_code=getattr(e, "returncode", -1),
                project_name=project_name,
            ),
            level=EventLevel.ERROR,
        )
        raise

    fire_event(
        V2ParserEnd(
            status="success",
            execution_time=time.monotonic() - start_time,
            error_class="",
            exit_code=-1,
            project_name=project_name,
        ),
        level=EventLevel.INFO,
    )

    manifest = Manifest.from_writable_manifest(writable_manifest)
    # build_flat_graph is normally called by ManifestLoader.get_full_manifest;
    # the fusion path bypasses that loader, so populate flat_graph here to
    # power the `graph` context variable (graph.nodes, graph.sources, ...).
    manifest.build_flat_graph()

    _delete_stale_partial_parse(project_target_path)

    if write and write_json:
        enrich_manifest_with_plugin_artifacts(manifest, runtime_config.project_name)

    return manifest


def _build_argv(flags, target_path_override: Optional[str] = None) -> List[str]:
    """Translate dbt-core flags into fusion parser CLI args.

    The base command is taken from flags.V2_PARSER (default
    'dbt-core-experimental-parser parse') and split with shlex so users can
    configure subcommands or wrappers.

    Forwarded flags (must affect manifest output):
      --project-dir, --profiles-dir, --profile, --target,
      --target-path, --vars, --packages-install-path

    When target_path_override is provided, it replaces the user's --target-path
    so the fusion parser writes its handoff manifest where dbt expects it (a
    temp dir).
    """
    # posix=False on Windows so backslashes in paths (e.g. C:\path\to\parser.exe)
    # aren't stripped as shell escapes.
    base = shlex.split(
        getattr(flags, "V2_PARSER", "dbt-core-experimental-parser parse"),
        posix=(os.name != "nt"),
    )
    # Expand `~` in the parser binary so users can point --v2-parser at e.g.
    # `~/bin/my-parser`. shlex.split treats `~` as literal.
    if base:
        base[0] = _resolve_engine_command(os.path.expanduser(base[0]))
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

    # Forward dbt-core's invocation_id so fs telemetry shares the same trace.
    invocation_id = get_invocation_id()
    if invocation_id:
        forwarded += ["--invocation-id", str(invocation_id)]

    return base + forwarded


def _resolve_engine_command(command: str) -> str:
    """Resolve a bare engine binary name to the wheel-installed path.

    If the user supplied an absolute/relative path or a multi-segment command,
    leave it alone. Otherwise, look for the binary in this Python install's
    scripts directory so we use the version pinned by dbt-core's dependency
    on dbt-core-experimental-parser rather than whatever's on PATH.
    """
    if os.sep in command or (os.altsep and os.altsep in command):
        return command
    name = f"{command}.exe" if platform.system() == "Windows" else command
    candidate = Path(sysconfig.get_path("scripts")) / name
    if candidate.exists():
        return str(candidate)
    return command


def _fusion_subprocess_env() -> dict:
    """Return env for the fs subprocess, overriding DBT_INVOCATION_ENV.

    Setting DBT_INVOCATION_ENV=dbt-core-v2-parser on the child only (not the
    parent process env) tags every fs telemetry record from this run so the
    internal-analytics warehouse can attribute it to the v2-parser pathway.
    The host orchestrator's DBT_INVOCATION_ENV (set by dbt platform Orc/Sinter
    or by CI) still applies to dbt-core's own telemetry — we only relabel the
    embedded fs run.

    Also strips every DBT_ENGINE_* env var that maps to a dbt-core CLI option:
    fs hard-errors on unknown DBT_ENGINE_* vars (its prefix is reserved), and
    parsing-relevant flags are forwarded via argv by _build_argv. The
    DBT_ENGINE_STATE_* / recorder / deps vars in _ADDITIONAL_ENGINE_ENV_VARS
    are not click-bound and pass through unchanged — fs uses them natively.
    """
    from dbt.cli import params

    env = os.environ.copy()
    for engine_env_var in params.KNOWN_ENV_VARS:
        env.pop(engine_env_var.name, None)
    env["DBT_INVOCATION_ENV"] = "dbt-core-v2-parser"
    return env


def _run_fusion(argv: List[str]) -> None:
    # Passthrough mode: the fusion parser inherits dbt's stdout/stderr so users
    # see progress and errors live. This bypasses dbt's event system (no
    # log-file capture, no --log-format json, no level filtering), but is the
    # only way to get streaming output without re-parsing the parser's
    # free-form text.
    #
    # TODO: replace with Popen + line-by-line streaming and re-emit through
    # fire_event once the fusion parser ships its structured (JSON) log stream
    # and the Python parsing library lands. Sketch:
    #   proc = subprocess.Popen(argv, stdout=PIPE, stderr=PIPE, text=True, bufsize=1)
    #   - read stdout/stderr concurrently (threads or selectors; a single
    #     blocking readline() on one stream deadlocks if the other fills its pipe)
    #   - parse each line as a structured log record
    #   - map level/message to a dbt event type and fire_event(...)
    #   - proc.wait() and check returncode
    # Until then, capturing-then-printing-at-end would lose streaming (fusion
    # parsing can take minutes on large projects), so we inherit fds instead.
    try:
        result = subprocess.run(argv, check=False, env=_fusion_subprocess_env())
    except FileNotFoundError as e:
        raise FusionParserError(
            f"Fusion parser command not found: {argv[0]!r}. "
            f"Reinstall dbt-core-experimental-parser, or set --v2-parser to "
            f"point to an alternate engine binary."
        ) from e

    if result.returncode != 0:
        raise FusionParserError(
            f"Fusion parser failed (exit {result.returncode}); see parser output above.",
            returncode=result.returncode,
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
    """Serialize the resolved --vars dict to a YAML string for the fusion parser.

    dbt-core's --vars is parsed into a dict by click via the YAML param type
    (cli/params.py vars). Forward as a compact YAML string so the fusion
    parser receives a single canonical value rather than re-resolving env
    vars or layered configs.
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
