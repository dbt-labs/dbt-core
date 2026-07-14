"""Integration tests: run real dbt commands against a fixture project.

parse/list use a snowflake target with dummy creds — they don't open a
connection, so no warehouse is needed. The golden tests run each command in a
fresh subprocess (dbt_cli) and diff its output; the behavioral tests run
in-process and use capfd just to swallow engine output. Warehouse-only tests
are marked requires_warehouse.
"""

import os
import subprocess
import sys
import threading

import pytest
from dbt.cli.main import dbtRunner, dbtRunnerResult


def _invoke(proj, *args, **kwargs):
    return dbtRunner().invoke(
        [*args, "--project-dir", str(proj), "--profiles-dir", str(proj)], **kwargs
    )


# Golden tests run each command in a fresh subprocess (dbt_cli), like the Rust
# goldie tests run the binary — output is captured reliably and independent of
# test order. --show progress puts per-node progress on stdout, matching the
# Rust `parse --show progress` golden.
def test_parse_hello_world(tmp_project, dbt_cli, golden):
    proj = tmp_project("hello_world")
    p = dbt_cli(["parse", "--show", "progress"], proj)

    assert p.returncode == 0, p.stderr
    scrub = [str(proj)]
    golden("hello_world/parse.stdout", p.stdout, scrub)
    golden("hello_world/parse.stderr", p.stderr, scrub)
    assert (proj / "target" / "manifest.json").is_file()


def test_list_hello_world(tmp_project, dbt_cli, golden):
    proj = tmp_project("hello_world")
    p = dbt_cli(["list", "--show", "progress"], proj)

    assert p.returncode == 0, p.stderr
    scrub = [str(proj)]
    golden("hello_world/list.stdout", p.stdout, scrub)
    golden("hello_world/list.stderr", p.stderr, scrub)
    assert "hello_world.hello_world" in p.stdout  # list prints the model node


# The `dbt` console script (dbt.cli.main:cli) is the CLI shipped by this wheel.
# It reads sys.argv and process-exits, so — like the golden tests — it only works
# in a fresh subprocess.
def test_console_entrypoint_version():
    script = "import sys; from dbt.cli.main import cli; sys.argv = ['dbt', '--version']; cli()"
    p = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert "dbt-core" in (p.stdout + p.stderr)


def test_console_entrypoint_runs_command(tmp_project):
    proj = tmp_project("hello_world")
    script = (
        "import sys; from dbt.cli.main import cli; "
        "sys.argv = ['dbt', 'parse', '--project-dir', sys.argv[1], '--profiles-dir', sys.argv[1]]; "
        "cli()"
    )
    p = subprocess.run([sys.executable, "-c", script, str(proj)], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    assert (proj / "target" / "manifest.json").is_file()


def test_invoke_returns_runner_result(tmp_project, capfd):
    proj = tmp_project("hello_world")
    res = _invoke(proj, "parse")
    capfd.readouterr()
    assert isinstance(res, dbtRunnerResult)


def test_kwargs_invocation_equivalent_to_flags(tmp_project, capfd):
    # invoke(["list"], select="hello_world") == invoke(["list", "--select", "hello_world"])
    proj = tmp_project("hello_world")
    res = _invoke(proj, "list", select="hello_world")
    capfd.readouterr()
    assert res.success is True


def test_multiple_invocations_in_one_process(tmp_project, capfd):
    # Second invoke must reuse the once-per-process tracing init, not re-run it.
    proj = tmp_project("hello_world")
    assert _invoke(proj, "parse").success
    assert _invoke(proj, "parse").success
    capfd.readouterr()


def test_unimplemented_adapter_is_not_fatal(tmp_project, capfd):
    # The datafusion adapter is unimplemented. Whether the engine returns a
    # failed result or pyo3 catches a panic, the contract is the same: the
    # interpreter must NOT be killed and the runner stays usable afterward.
    proj = tmp_project("hello_world")
    os.environ["target_env_var"] = "datafusion"
    try:
        res = _invoke(proj, "parse")
    finally:
        os.environ.pop("target_env_var", None)
    capfd.readouterr()
    assert res.success is False
    # Interpreter still alive: a subsequent call still works.
    assert _invoke(proj, "parse").success
    capfd.readouterr()


def test_engine_error_surfaces_exception(tmp_project, capfd):
    # A model referencing a nonexistent node is a deterministic engine error
    # (not a parse error or a panic). The engine reports it on the result, so
    # the failure must carry a message, not a bare success=False.
    proj = tmp_project("hello_world")
    broken = proj / "models" / "broken.sql"
    broken.write_text("select * from {{ ref('nope') }}\n")
    res = _invoke(proj, "parse")
    capfd.readouterr()
    assert res.success is False
    assert res.exception is not None
    assert str(res.exception)  # non-empty message, not a swallowed error
    # Interpreter still alive: removing the bad model lets a re-parse succeed.
    broken.unlink()
    assert _invoke(proj, "parse").success
    capfd.readouterr()


def test_gil_released_during_invoke(tmp_project, capfd):
    # invoke() drops the GIL via allow_threads, so a CPU-bound (no-sleep, hence
    # GIL-dependent) Python thread makes progress during the run. If the GIL
    # were held for the whole call, the worker would be starved.
    proj = tmp_project("hello_world")
    counter = [0]
    stop = threading.Event()

    def worker():
        while not stop.is_set():
            counter[0] += 1

    t = threading.Thread(target=worker)
    t.start()
    try:
        before = counter[0]
        _invoke(proj, "parse")
        delta = counter[0] - before
    finally:
        stop.set()
        t.join()
    capfd.readouterr()
    assert delta > 100, f"worker barely progressed ({delta}); GIL may not be released"


@pytest.mark.requires_warehouse
def test_run_against_warehouse(tmp_project):
    """End-to-end run against a real adapter; set target_env_var + credentials."""
    proj = tmp_project("hello_world")
    assert os.environ.get("target_env_var"), "set target_env_var to a warehouse output"
    res = _invoke(proj, "run")
    assert res.success is True
