from typing import Dict, Optional, Tuple
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


def print_fancy_output_line(
        msg: str, status: str, index: Optional[int], total: Optional[int],
        execution_time: Optional[float] = None, truncate: bool = False
) -> None:
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(),
        progress=progress,
        message=msg)

    truncate_width = ui.PRINTER_WIDTH - 3
    justified = prefix.ljust(ui.PRINTER_WIDTH, ".")
    if truncate and len(justified) > truncate_width:
        justified = justified[:truncate_width] + '...'

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(
            execution_time=execution_time)

    output = "{justified} [{status}{status_time}]".format(
        justified=justified, status=status, status_time=status_time)

    logger.info(output)


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
    print_fancy_output_line(msg, 'RUN', index, total)


def print_hook_start_line(statement: str, index: int, total: int) -> None:
    msg = 'START hook: {}'.format(statement)
    print_fancy_output_line(msg, 'RUN', index, total, truncate=True)


def print_hook_end_line(
    statement: str, status: str, index: int, total: int, execution_time: float
) -> None:
    msg = 'OK hook: {}'.format(statement)
    # hooks don't fail into this path, so always green
    print_fancy_output_line(msg, ui.green(status), index, total,
                            execution_time=execution_time, truncate=True)


def print_skip_line(
    model, schema: str, relation: str, index: int, num_models: int
) -> None:
    msg = 'SKIP relation {}.{}'.format(schema, relation)
    print_fancy_output_line(msg, ui.yellow('SKIP'), index, num_models)


def print_cancel_line(model) -> None:
    msg = 'CANCEL query {}'.format(model)
    print_fancy_output_line(msg, ui.red('CANCEL'), index=None, total=None)


def get_printable_result(result, success: str, error: str) -> Tuple[str, str]:
    if result.error is not None:
        info = 'ERROR {}'.format(error)
        status = ui.red(result.status)
    else:
        info = 'OK {}'.format(success)
        status = ui.green(result.status)

    return info, status


def print_test_result_line(
        result, schema_name, index: int, total: int
) -> None:
    model = result.node

    if result.error is not None:
        info = "ERROR"
        color = ui.red
    elif result.status == 0:
        info = 'PASS'
        color = ui.green

    elif result.warn:
        info = 'WARN {}'.format(result.status)
        color = ui.yellow

    elif result.fail:
        info = 'FAIL {}'.format(result.status)
        color = ui.red

    else:
        raise RuntimeError("unexpected status: {}".format(result.status))

    print_fancy_output_line(
        "{info} {name}".format(info=info, name=model.name),
        color(info),
        index,
        total,
        result.execution_time)


def print_model_result_line(
    result, description: str, index: int, total: int
) -> None:
    info, status = get_printable_result(result, 'created', 'creating')

    print_fancy_output_line(
        "{info} {description}".format(info=info, description=description),
        status,
        index,
        total,
        result.execution_time)


def print_snapshot_result_line(
    result, description: str, index: int, total: int
) -> None:
    model = result.node

    info, status = get_printable_result(result, 'snapshotted', 'snapshotting')
    cfg = model.config.to_dict()

    msg = "{info} {description}".format(
        info=info, description=description, **cfg)
    print_fancy_output_line(
        msg,
        status,
        index,
        total,
        result.execution_time)


def print_seed_result_line(result, schema_name: str, index: int, total: int):
    model = result.node

    info, status = get_printable_result(result, 'loaded', 'loading')

    print_fancy_output_line(
        "{info} seed file {schema}.{relation}".format(
            info=info,
            schema=schema_name,
            relation=model.alias),
        status,
        index,
        total,
        result.execution_time)


def print_freshness_result_line(result, index: int, total: int) -> None:
    if result.error:
        info = 'ERROR'
        color = ui.red
    elif result.status == 'error':
        info = 'ERROR STALE'
        color = ui.red
    elif result.status == 'warn':
        info = 'WARN'
        color = ui.yellow
    else:
        info = 'PASS'
        color = ui.green

    if hasattr(result, 'node'):
        source_name = result.node.source_name
        table_name = result.node.name
    else:
        source_name = result.source_name
        table_name = result.table_name

    msg = "{info} freshness of {source_name}.{table_name}".format(
        info=info,
        source_name=source_name,
        table_name=table_name
    )

    print_fancy_output_line(
        msg,
        color(info),
        index,
        total,
        execution_time=result.execution_time
    )


def interpret_run_result(result) -> str:
    if result.error is not None or result.fail:
        return 'error'
    elif result.skipped:
        return 'skip'
    elif result.warn:
        return 'warn'
    else:
        return 'pass'


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

    if result.fail or (is_warning and result.warn):
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

        try:
            int(result.status)
        except ValueError:
            logger.error("  Status: {}".format(result.status))
        else:
            status = utils.pluralize(result.status, 'result')
            logger.error("  Got {}, expected 0.".format(status))

        if result.node.build_path is not None:
            with TextOnly():
                logger.info("")
            logger.info("  compiled SQL at {}".format(
                result.node.build_path))

    else:
        first = True
        for line in result.error.split("\n"):
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
    print_fancy_output_line(msg, ui.red('ERROR SKIP'), index, num_models)
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
    errors = [r for r in results if r.error is not None or r.fail]
    warnings = [r for r in results if r.warn]
    with DbtStatusMessage(), InvocationProcessor():
        print_end_of_run_summary(len(errors),
                                 len(warnings),
                                 keyboard_interrupt)

        for error in errors:
            print_run_result_error(error, is_warning=False)

        for warning in warnings:
            print_run_result_error(warning, is_warning=True)

        print_run_status_line(results)
