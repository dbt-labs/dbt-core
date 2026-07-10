from __future__ import annotations

from typing import Any, List, Optional

from dbt._core import DbtRunner as _DbtRunner
from dbt._core import run_cli as _run_cli


def cli() -> None:
    """Console-script entrypoint for the ``dbt`` command.

    Hands ``sys.argv`` to the compiled engine, which runs the command in-process
    and exits the process — this never returns.
    """
    import sys

    _run_cli(sys.argv)


class DbtRunnerError(Exception):
    """An error raised by the dbt engine, carried on ``dbtRunnerResult.exception``.

    The engine reports errors as a message string rather than a typed Python
    exception, so this wraps that message.
    """


class dbtRunnerResult:
    """Result of a dbt invocation.

    success: True if the command exited 0.
    result: command-specific artifact object (Manifest / RunResultsArtifact /
        CatalogArtifact), or None for commands with no in-memory artifact.
    exception: set only on an error or a caught Rust panic (we never let it
        kill the interpreter). A handled failure like a failing test is
        success=False with no exception.
    exit_code: raw engine exit code (0 ok, 1 failure, 2 error).
    """

    def __init__(
        self,
        success: bool,
        result: Any = None,
        exception: Optional[BaseException] = None,
        exit_code: Optional[int] = None,
    ):
        self.success = success
        self.result = result
        self.exception = exception
        self.exit_code = exit_code

    def __repr__(self) -> str:
        return (
            f"dbtRunnerResult(success={self.success}, "
            f"result={self.result!r}, exception={self.exception!r})"
        )


def _kwargs_to_cli(kwargs: dict) -> List[str]:
    """Turn keyword args into CLI flags.

        fail_fast=True     -> --fail-fast
        fail_fast=False    -> --no-fail-fast
        select="my_model" -> --select my_model
        select=["a", "b"] -> --select a --select b
        threads=4          -> --threads 4

    An unknown flag just becomes a parse error on the result, not a crash.
    """
    argv: List[str] = []
    for key, value in kwargs.items():
        flag = "--" + key.replace("_", "-")
        if isinstance(value, bool):
            argv.append(flag if value else "--no-" + key.replace("_", "-"))
        elif isinstance(value, (list, tuple)):
            for item in value:
                argv += [flag, str(item)]
        else:
            argv += [flag, str(value)]
    return argv


class dbtRunner:
    """In-process dbt runner. Reuse one instance across calls."""

    # Known limitation: logging is set up once per process, on the first
    # invoke(). The log-file destination and level are fixed from that point,
    # so invoking a different project (or different log settings) in the same
    # process still logs to the first command's logs/dbt.log. Use a fresh
    # process for per-project log files. (Legacy dbt-core reconfigured logging
    # each invoke; restoring that is a tracked follow-up.)

    def __init__(self, manifest: Any = None, callbacks: Any = None):
        if manifest is not None:
            raise NotImplementedError(
                "manifest= injection is not yet supported. Reuse the runner "
                "instance across invocations to avoid re-parsing."
            )
        if callbacks is not None:
            raise NotImplementedError("callbacks= (EventManager hooks) are not yet supported.")
        self._runner = _DbtRunner()

    def invoke(self, args: List[str], **kwargs) -> dbtRunnerResult:
        argv = list(args) + _kwargs_to_cli(kwargs)
        try:
            core = self._runner.invoke(argv)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:
            # Parse errors and caught panics land here — hand them back on the
            # result instead of letting them kill the interpreter.
            return dbtRunnerResult(success=False, result=None, exception=exc)
        # The engine reports errors on the result (exit_code + message) rather
        # than raising, so surface the message as an exception here instead of
        # discarding it. None when the command errored out cleanly.
        exception = DbtRunnerError(core.exception) if core.exception else None
        return dbtRunnerResult(
            success=core.success,
            # Command-specific contract object (Manifest / RunResultsArtifact /
            # CatalogArtifact), or None for commands with no in-memory artifact.
            result=core.result,
            exception=exception,
            exit_code=core.exit_code,
        )
