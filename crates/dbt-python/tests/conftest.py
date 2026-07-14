"""Shared pytest fixtures.

The integration tests run dbt in-process and diff captured output against
golden files, the same way the Rust goldie tests do. Regenerate goldens with
``cargo xtask test-py --goldie-update`` (sets ``GOLDIE_UPDATE=1``, the env var
the Rust side reads too).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
FIXTURES_DIR = TESTS_DIR / "fixtures"
GOLDEN_DIR = TESTS_DIR / "golden"


def _golden_update_enabled() -> bool:
    return os.environ.get("GOLDIE_UPDATE") == "1"


# Scrub run-to-run noise so output is stable. Roughly tracks goldie.rs's
# postprocess_actual on the Rust side.
_NORMALIZERS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\x1b\[[0-9;]*m"), ""),  # ANSI color codes
    (re.compile(r"\b\d+(?:\.\d+)?\s?(?:ms|µs|us|s|m)\b"), "[duration]"),
    (re.compile(r"\b\d+\.\d+\.\d+(?:[-.a-zA-Z0-9]+)?"), "[version]"),
    (re.compile(r"0x[0-9a-fA-F]+"), "0x[id]"),
    (re.compile(r"inline_[0-9a-f]+\.sql"), "inline_[hash].sql"),
]

# Noise lines to drop entirely. Defensive — the "shut down" warning shouldn't
# appear anymore now that the binding passes shutdown=false.
_DROP_LINE_PATTERNS = [
    re.compile(r"trying to log a message via Vortex"),
]


def _normalize(text: str, scrub_paths: list[str]) -> str:
    text = "\n".join(
        line for line in text.splitlines() if not any(p.search(line) for p in _DROP_LINE_PATTERNS)
    )
    # Longest path first so a shorter prefix doesn't clobber a longer one.
    for path in sorted(scrub_paths, key=len, reverse=True):
        if path:
            text = text.replace(path, "[project]")
    # Catch any temp path we didn't pass in explicitly.
    text = re.sub(r"/private/var/folders/[^\s\"']+", "[tmp]", text)
    text = re.sub(r"/tmp/[^\s\"']+", "[tmp]", text)
    for pattern, repl in _NORMALIZERS:
        text = pattern.sub(repl, text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    return text.strip("\n") + "\n" if text else text


def _assert_or_update(name: str, content: str, scrub_paths: list[str]) -> None:
    normalized = _normalize(content, scrub_paths)
    path = GOLDEN_DIR / name
    if _golden_update_enabled():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized)
        return
    assert path.exists(), (
        f"missing golden {path}; regenerate with `cargo xtask test-py --goldie-update`"
    )
    expected = path.read_text()
    assert normalized == expected, (
        f"golden mismatch for {name}\n"
        f"--- expected ({path}) ---\n{expected}\n"
        f"--- actual ---\n{normalized}\n"
        f"if this change is intended, rerun with --goldie-update."
    )


@pytest.fixture
def golden():
    """Returns ``check(name, content, scrub_paths=[])`` for golden comparison."""

    def check(name: str, content: str, scrub_paths: list[str] | None = None) -> None:
        _assert_or_update(name, content, scrub_paths or [])

    return check


@pytest.fixture
def tmp_project(tmp_path):
    """Copy a fixture project into a fresh tmp dir so tests don't share target/ state."""

    def _copy(name: str) -> Path:
        dst = tmp_path / name
        shutil.copytree(FIXTURES_DIR / name, dst)
        return dst

    return _copy


@pytest.fixture
def dbt_cli():
    # Run a command in a fresh subprocess and capture stdout/stderr — how the
    # Rust goldie tests run the binary. A fresh process gets a fresh tracing
    # init, so output is captured reliably regardless of test order; in-process
    # capture isn't, because the binding pins tracing to the first invoke's fd.
    script = (
        "import sys; from dbt.cli.main import dbtRunner; "
        "sys.exit(dbtRunner().invoke(sys.argv[1:]).exit_code or 0)"
    )

    def run(args, project):
        argv = [str(a) for a in args] + [
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
        ]
        return subprocess.run([sys.executable, "-c", script, *argv], capture_output=True, text=True)

    return run


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_warehouse: integration test that needs live adapter credentials "
        "(a real warehouse target); skipped in hermetic runs.",
    )
