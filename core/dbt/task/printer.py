from typing import Dict
from dbt.logger import (
    DbtStatusMessage,
    TextOnly,
)
from dbt.events.functions import fire_event
from dbt.events.types import (
    Formatting,
    RunResultWarning,
    RunResultWarningMessage,
    RunResultFailure,
    StatsLine,
    RunResultError,
    RunResultErrorNoMessage,
    SQLCompiledPath,
    CheckNodeTestFailure,
    EndOfRunSummary,
)

from dbt.tracking import InvocationProcessor
from dbt.events.format import pluralize

from dbt.contracts.results import NodeStatus
from dbt.node_types import NodeType


def get_counts(flat_nodes) -> str:
    counts: Dict[str, int] = {}

    for node in flat_nodes:
        t = node.resource_type

        if node.resource_type == NodeType.Model:
            t = "{} {}".format(node.get_materialization(), t)
        elif node.resource_type == NodeType.Operation:
            t = "hook"

        counts[t] = counts.get(t, 0) + 1

    stat_line = ", ".join([pluralize(v, k) for k, v in counts.items()])

    return stat_line


def interpret_run_result(result) -> str:
    if result.status in (NodeStatus.Error, NodeStatus.Fail):
        return "error"
    elif result.status == NodeStatus.Skipped:
        return "skip"
    elif result.status == NodeStatus.Warn:
        return "warn"
    elif result.status in (NodeStatus.Pass, NodeStatus.Success):
        return "pass"
    else:
        raise RuntimeError(f"unhandled result {result}")


def print_run_status_line(results) -> None:
    stats = {
        "error": 0,
        "skip": 0,
        "pass": 0,
        "warn": 0,
        "total": 0,
    }

    for r in results:
        result_type = interpret_run_result(r)
        stats[result_type] += 1
        stats["total"] += 1

    with TextOnly():
        fire_event(Formatting(""))
    fire_event(StatsLine(stats=stats))


def print_run_result_error(result, newline: bool = True, is_warning: bool = False) -> None:
    if newline:
        with TextOnly():
            fire_event(Formatting(""))

    if result.status == NodeStatus.Fail or (is_warning and result.status == NodeStatus.Warn):
        if is_warning:
            fire_event(
                RunResultWarning(
                    resource_type=result.node.resource_type,
                    node_name=result.node.name,
                    path=result.node.original_file_path,
                )
            )
        else:
            fire_event(
                RunResultFailure(
                    resource_type=result.node.resource_type,
                    node_name=result.node.name,
                    path=result.node.original_file_path,
                )
            )

        if result.message:
            if is_warning:
                fire_event(RunResultWarningMessage(msg=result.message))
            else:
                fire_event(RunResultError(msg=result.message))
        else:
            fire_event(RunResultErrorNoMessage(status=result.status))

        if result.node.build_path is not None:
            with TextOnly():
                fire_event(Formatting(""))
            fire_event(SQLCompiledPath(path=result.node.compiled_path))

        if result.node.should_store_failures:
            with TextOnly():
                fire_event(Formatting(""))
            fire_event(CheckNodeTestFailure(relation_name=result.node.relation_name))

    elif result.message is not None:
        fire_event(RunResultError(msg=result.message))


def print_run_end_messages(results, keyboard_interrupt: bool = False) -> None:
    errors, warnings = [], []
    for r in results:
        if r.status in (NodeStatus.RuntimeErr, NodeStatus.Error, NodeStatus.Fail):
            errors.append(r)
        elif r.status == NodeStatus.Skipped and r.message is not None:
            # this means we skipped a node because of an issue upstream,
            # so include it as an error
            errors.append(r)
        elif r.status == NodeStatus.Warn:
            warnings.append(r)

    with DbtStatusMessage(), InvocationProcessor():
        with TextOnly():
            fire_event(Formatting(""))
        fire_event(
            EndOfRunSummary(
                num_errors=len(errors),
                num_warnings=len(warnings),
                keyboard_interrupt=keyboard_interrupt,
            )
        )

        for error in errors:
            print_run_result_error(error, is_warning=False)

        for warning in warnings:
            print_run_result_error(warning, is_warning=True)

        print_run_status_line(results)
