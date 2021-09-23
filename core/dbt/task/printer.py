from typing import Dict, Optional, Tuple, Callable
from dbt.logger import (
    GLOBAL_LOGGER as logger,
    DbtStatusMessage,
    TextOnly,
    get_timestamp,
)
from dbt.node_types import NodeType

from dbt.tracking import InvocationProcessor
from dbt import ui
from dbt import utils

from dbt.contracts.results import (
    FreshnessStatus, NodeStatus, TestStatus
)


def print_fancy_output_line(
        msg: str, status: str, logger_fn: Callable, index: Optional[int],
        total: Optional[int], execution_time: Optional[float] = None,
        truncate: bool = False
) -> None:
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(),
        progress=progress,
        message=msg)

    truncate_width = ui.printer_width() - 3
    justified = prefix.ljust(ui.printer_width(), ".")
    if truncate and len(justified) > truncate_width:
        justified = justified[:truncate_width] + '...'

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(
            execution_time=execution_time)

    output = "{justified} [{status}{status_time}]".format(
        justified=justified, status=status, status_time=status_time)

    logger_fn(output)


def get_counts(flat_nodes) -> str:
    counts: Dict[str, int] = {}

    for node in flat_nodes:
        t = node.resource_type

        if node.resource_type == NodeType.Model:
            t = '{} {}'.format(node.get_materialization(), t)
        elif node.resource_type == NodeType.Operation:
            t = 'hook'

        counts[t] = counts.get(t, 0) + 1

    stat_line = ", ".join(
        [utils.pluralize(v, k) for k, v in counts.items()])

    return stat_line


def print_start_line(description: str, index: int, total: int) -> None:
    msg = "START {}".format(description)
    print_fancy_output_line(msg, 'RUN', logger.info, index, total)


def print_hook_start_line(statement: str, index: int, total: int) -> None:
    msg = 'START hook: {}'.format(statement)
    print_fancy_output_line(
        msg, 'RUN', logger.info, index, total, truncate=True)


def print_hook_end_line(
    statement: str, status: str, index: int, total: int, execution_time: float
) -> None:
    msg = 'OK hook: {}'.format(statement)
    # hooks don't fail into this path, so always green
    print_fancy_output_line(msg, ui.green(status), logger.info, index, total,
                            execution_time=execution_time, truncate=True)


def print_skip_line(
    node, schema: str, relation: str, index: int, num_models: int
) -> None:
    if node.resource_type in NodeType.refable():
        msg = f'SKIP relation {schema}.{relation}'
    else:
        msg = f'SKIP {node.resource_type} {node.name}'
    print_fancy_output_line(
        msg, ui.yellow('SKIP'), logger.info, index, num_models)


def print_cancel_line(model) -> None:
    msg = 'CANCEL query {}'.format(model)
    print_fancy_output_line(
        msg, ui.red('CANCEL'), logger.error, index=None, total=None)


def get_printable_result(
        result, success: str, error: str) -> Tuple[str, str, Callable]:
    if result.status == NodeStatus.Error:
        info = 'ERROR {}'.format(error)
        status = ui.red(result.status.upper())
        logger_fn = logger.error
    else:
        info = 'OK {}'.format(success)
        status = ui.green(result.message)
        logger_fn = logger.info

    return info, status, logger_fn


def print_test_result_line(
        result, index: int, total: int
) -> None:
    model = result.node

    if result.status == TestStatus.Error:
        info = "ERROR"
        color = ui.red
        logger_fn = logger.error
    elif result.status == TestStatus.Pass:
        info = 'PASS'
        color = ui.green
        logger_fn = logger.info
    elif result.status == TestStatus.Warn:
        info = f'WARN {result.failures}'
        color = ui.yellow
        logger_fn = logger.warning
    elif result.status == TestStatus.Fail:
        info = f'FAIL {result.failures}'
        color = ui.red
        logger_fn = logger.error
    else:
        raise RuntimeError("unexpected status: {}".format(result.status))

    print_fancy_output_line(
        "{info} {name}".format(info=info, name=model.name),
        color(info),
        logger_fn,
        index,
        total,
        result.execution_time)


def print_model_result_line(
    result, description: str, index: int, total: int
) -> None:
    info, status, logger_fn = get_printable_result(
        result, 'created', 'creating')

    print_fancy_output_line(
        "{info} {description}".format(info=info, description=description),
        status,
        logger_fn,
        index,
        total,
        result.execution_time)


def print_snapshot_result_line(
    result, description: str, index: int, total: int
) -> None:
    model = result.node

    info, status, logger_fn = get_printable_result(
        result, 'snapshotted', 'snapshotting')
    cfg = model.config.to_dict(omit_none=True)

    msg = "{info} {description}".format(
        info=info, description=description, **cfg)
    print_fancy_output_line(
        msg,
        status,
        logger_fn,
        index,
        total,
        result.execution_time)


def print_seed_result_line(result, schema_name: str, index: int, total: int):
    model = result.node

    info, status, logger_fn = get_printable_result(result, 'loaded', 'loading')

    print_fancy_output_line(
        "{info} seed file {schema}.{relation}".format(
            info=info,
            schema=schema_name,
            relation=model.alias),
        status,
        logger_fn,
        index,
        total,
        result.execution_time)


def print_freshness_result_line(result, index: int, total: int) -> None:
    if result.status == FreshnessStatus.RuntimeErr:
        info = 'ERROR'
        color = ui.red
        logger_fn = logger.error
    elif result.status == FreshnessStatus.Error:
        info = 'ERROR STALE'
        color = ui.red
        logger_fn = logger.error
    elif result.status == FreshnessStatus.Warn:
        info = 'WARN'
        color = ui.yellow
        logger_fn = logger.warning
    else:
        info = 'PASS'
        color = ui.green
        logger_fn = logger.info

    if hasattr(result, 'node'):
        source_name = result.node.source_name
        table_name = result.node.name
    else:
        source_name = result.source_name
        table_name = result.table_name

    msg = f"{info} freshness of {source_name}.{table_name}"

    print_fancy_output_line(
        msg,
        color(info),
        logger_fn,
        index,
        total,
        execution_time=result.execution_time
    )


def interpret_run_result(result) -> str:
    if result.status in (NodeStatus.Error, NodeStatus.Fail):
        return 'error'
    elif result.status == NodeStatus.Skipped:
        return 'skip'
    elif result.status == NodeStatus.Warn:
        return 'warn'
    elif result.status in (NodeStatus.Pass, NodeStatus.Success):
        return 'pass'
    else:
        raise RuntimeError(f"unhandled result {result}")


def print_run_status_line(results) -> None:
    stats = {
        'error': 0,
        'skip': 0,
        'pass': 0,
        'warn': 0,
        'total': 0,
    }

    for r in results:
        result_type = interpret_run_result(r)
        stats[result_type] += 1
        stats['total'] += 1

    stats_line = "\nDone. PASS={pass} WARN={warn} ERROR={error} SKIP={skip} TOTAL={total}"  # noqa
    logger.info(stats_line.format(**stats))


def print_run_result_error(
    result, newline: bool = True, is_warning: bool = False
) -> None:
    if newline:
        with TextOnly():
            logger.info("")

    if result.status == NodeStatus.Fail or (
        is_warning and result.status == NodeStatus.Warn
    ):
        if is_warning:
            color = ui.yellow
            info = 'Warning'
            logger_fn = logger.warning
        else:
            color = ui.red
            info = 'Failure'
            logger_fn = logger.error
        logger_fn(color("{} in {} {} ({})").format(
            info,
            result.node.resource_type,
            result.node.name,
            result.node.original_file_path))

        if result.message:
            logger.error(f"  {result.message}")
        else:
            logger.error(f"  Status: {result.status}")

        if result.node.build_path is not None:
            with TextOnly():
                logger.info("")
            logger.info("  compiled SQL at {}".format(
                result.node.compiled_path))

        if result.node.should_store_failures:
            with TextOnly():
                logger.info("")
            msg = f"select * from {result.node.relation_name}"
            border = '-' * len(msg)
            logger.info(f"  See test failures:\n  {border}\n  {msg}\n  {border}")

    elif result.message is not None:
        first = True
        for line in result.message.split("\n"):
            if first:
                logger.error(ui.yellow(line))
                first = False
            else:
                logger.error(line)


def print_skip_caused_by_error(
    model, schema: str, relation: str, index: int, num_models: int, result
) -> None:
    msg = ('SKIP relation {}.{} due to ephemeral model error'
           .format(schema, relation))
    print_fancy_output_line(
        msg, ui.red('ERROR SKIP'), logger.error, index, num_models)
    print_run_result_error(result, newline=False)


def print_end_of_run_summary(
    num_errors: int, num_warnings: int, keyboard_interrupt: bool = False
) -> None:
    error_plural = utils.pluralize(num_errors, 'error')
    warn_plural = utils.pluralize(num_warnings, 'warning')
    if keyboard_interrupt:
        message = ui.yellow('Exited because of keyboard interrupt.')
    elif num_errors > 0:
        message = ui.red("Completed with {} and {}:".format(
            error_plural, warn_plural))
    elif num_warnings > 0:
        message = ui.yellow('Completed with {}:'.format(warn_plural))
    else:
        message = ui.green('Completed successfully')

    with TextOnly():
        logger.info('')
    logger.info('{}'.format(message))


def print_run_end_messages(results, keyboard_interrupt: bool = False) -> None:
    errors, warnings = [], []
    for r in results:
        if r.status in (
            NodeStatus.RuntimeErr,
            NodeStatus.Error,
            NodeStatus.Fail
        ):
            errors.append(r)
        elif r.status == NodeStatus.Skipped and r.message is not None:
            # this means we skipped a node because of an issue upstream,
            # so include it as an error
            errors.append(r)
        elif r.status == NodeStatus.Warn:
            warnings.append(r)

    with DbtStatusMessage(), InvocationProcessor():
        print_end_of_run_summary(len(errors),
                                 len(warnings),
                                 keyboard_interrupt)

        for error in errors:
            print_run_result_error(error, is_warning=False)

        for warning in warnings:
            print_run_result_error(warning, is_warning=True)

        print_run_status_line(results)
