"""Unit tests for the dbtRunner / dbtRunnerResult wrapper.

No dbt project here — import paths, result fields, kwargs mapping, the
NotImplementedError rejections, and exception capture. Engine runs live in
test_commands.py.
"""

import pytest
from dbt.cli.main import _kwargs_to_cli, dbtRunner, dbtRunnerResult


def test_canonical_import_path():
    # The legacy dbt-core import must resolve from dbt.cli.main.
    from dbt.cli.main import dbtRunner as R  # noqa: N814
    from dbt.cli.main import dbtRunnerResult as Res

    assert R is dbtRunner and Res is dbtRunnerResult


def test_legacy_runner_alias():
    from dbt.runner import dbtRunner as R  # noqa: N814
    from dbt.runner import dbtRunnerResult as Res

    assert R is dbtRunner and Res is dbtRunnerResult


def test_result_contract_fields():
    res = dbtRunnerResult(success=True, result=None, exception=None, exit_code=0)
    assert res.success is True
    assert res.result is None
    assert res.exception is None
    assert res.exit_code == 0


def test_runner_constructs():
    assert dbtRunner() is not None


def test_manifest_injection_not_implemented():
    with pytest.raises(NotImplementedError, match="manifest="):
        dbtRunner(manifest=object())


def test_callbacks_not_implemented():
    with pytest.raises(NotImplementedError, match="callbacks="):
        dbtRunner(callbacks=[lambda event: None])


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"fail_fast": True}, ["--fail-fast"]),
        ({"fail_fast": False}, ["--no-fail-fast"]),
        ({"select": "my_model"}, ["--select", "my_model"]),
        ({"select": ["a", "b"]}, ["--select", "a", "--select", "b"]),
        ({"threads": 4}, ["--threads", "4"]),
    ],
)
def test_kwargs_to_cli_mapping(kwargs, expected):
    assert _kwargs_to_cli(kwargs) == expected


def test_unknown_command_captured_as_exception():
    # A parse failure must NOT raise or kill the interpreter — it is reported
    # on the result object.
    res = dbtRunner().invoke(["this-is-not-a-command"])
    assert res.success is False
    assert res.exception is not None
    assert isinstance(res.exception, BaseException)
