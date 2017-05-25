
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import get_materialization, NodeType
import dbt.ui.colors

import time

USE_COLORS = False

COLOR_FG_RED = dbt.ui.colors.COLORS['red']
COLOR_FG_GREEN = dbt.ui.colors.COLORS['green']
COLOR_FG_YELLOW = dbt.ui.colors.COLORS['yellow']
COLOR_RESET_ALL = dbt.ui.colors.COLORS['reset_all']


def use_colors():
    global USE_COLORS
    USE_COLORS = True


def get_timestamp():
    return time.strftime("%H:%M:%S")


def color(text, color_code):
    if USE_COLORS:
        return "{}{}{}".format(color_code, text, COLOR_RESET_ALL)
    else:
        return text


def green(text):
    return color(text, COLOR_FG_GREEN)


def yellow(text):
    return color(text, COLOR_FG_YELLOW)


def red(text):
    return color(text, COLOR_FG_RED)


def print_timestamped_line(msg):
    logger.info("{} | {}".format(get_timestamp(), msg))


def print_fancy_output_line(msg, status, index, total, execution_time=None):
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=get_timestamp(),
        progress=progress,
        message=msg)

    justified = prefix.ljust(80, ".")

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(
            execution_time=execution_time)

    status_txt = status

    output = "{justified} [{status}{status_time}]".format(
        justified=justified, status=status_txt, status_time=status_time)

    logger.info(output)


def print_skip_line(model, schema, relation, index, num_models):
    msg = 'SKIP relation {}.{}'.format(schema, relation)
    print_fancy_output_line(msg, yellow('SKIP'), index, num_models)


def print_cancel_line(model, schema):
    msg = 'CANCEL query {}.{}'.format(schema, model)
    print_fancy_output_line(msg, red('CANCEL'), index=None, total=None)


def get_counts(flat_nodes):
    counts = {}

    for node in flat_nodes:
        t = node.get('resource_type')

        if node.get('resource_type') == NodeType.Model:
            t = '{} {}'.format(get_materialization(node), t)

        counts[t] = counts.get(t, 0) + 1

    stat_line = ", ".join(
        ["{} {}s".format(v, k) for k, v in counts.items()])

    return stat_line


def print_test_start_line(model, schema_name, index, total):
    msg = "START test {name}".format(
        name=model.get('name'))

    run = 'RUN'
    print_fancy_output_line(msg, run, index, total)


def print_model_start_line(model, schema_name, index, total):
    msg = "START {model_type} model {schema}.{relation}".format(
        model_type=get_materialization(model),
        schema=schema_name,
        relation=model.get('name'))

    run = 'RUN'
    print_fancy_output_line(msg, run, index, total)


def print_archive_start_line(model, index, total):
    cfg = model.get('config', {})
    msg = "START archive {source_schema}.{source_table} --> "\
          "{target_schema}.{target_table}".format(**cfg)

    run = 'RUN'
    print_fancy_output_line(msg, run, index, total)


def print_test_result_line(result, schema_name, index, total):
    model = result.node
    info = 'PASS'

    if result.errored:
        info = "ERROR"
        color = red

    elif result.status > 0:
        info = 'FAIL {}'.format(result.status)
        color = red

        result.fail = True
    elif result.status == 0:
        info = 'PASS'
        color = green

    else:
        raise RuntimeError("unexpected status: {}".format(result.status))

    print_fancy_output_line(
        "{info} {name}".format(info=info, name=model.get('name')),
        color(info),
        index,
        total,
        result.execution_time)


def get_printable_result(result, success, error):
    if result.errored:
        info = 'ERROR {}'.format(error)
        status = red(result.status)
    else:
        info = 'OK {}'.format(success)
        status = green(result.status)

    return info, status


def print_archive_result_line(result, index, total):
    model = result.node

    info, status = get_printable_result(result, 'archived', 'archiving')
    cfg = model.get('config', {})

    print_fancy_output_line(
        "{info} {source_schema}.{source_table} --> "
        "{target_schema}.{target_table}".format(info=info, **cfg),
        status,
        index,
        total,
        result.execution_time)


def print_model_result_line(result, schema_name, index, total):
    model = result.node

    info, status = get_printable_result(result, 'created', 'creating')

    print_fancy_output_line(
        "{info} {model_type} model {schema}.{relation}".format(
            info=info,
            model_type=get_materialization(model),
            schema=schema_name,
            relation=model.get('name')),
        status,
        index,
        total,
        result.execution_time)


def interpret_run_result(result):
    if result.errored or result.failed:
        return 'error'
    elif result.skipped:
        return 'skip'
    else:
        return 'pass'


def print_run_status_line(results):
    stats = {
        'error': 0,
        'skip': 0,
        'pass': 0,
        'total': 0,
    }

    for r in results:
        result_type = interpret_run_result(r)
        stats[result_type] += 1
        stats['total'] += 1

    stats_line = "\nDone. PASS={pass} ERROR={error} SKIP={skip} TOTAL={total}"
    logger.info(stats_line.format(**stats))


def print_run_result_error(result):
    node = result.node

    if result.failed:
        status = 'FAIL {}'.format(result.status)
    else:
        status = result.status

    msg = " - {status} in {type} {package_name}.{node_name} ({path})".format(
        status=red(status),
        type=node.get('resource_type'),
        package_name=node.get('package_name'),
        node_name=node.get('name'),
        path=node.get('build_path')
    )
    logger.info(msg)


def print_end_of_run_summary(num_errors):
    if num_errors > 0:
        message = red('Completed with {} errors:'.format(num_errors))
    else:
        message = green('Completed successfully')

    logger.info('\n{}'.format(message))


def print_run_end_messages(results):
    errors = [r for r in results if r.errored or r.failed]
    print_end_of_run_summary(len(errors))

    for error in errors:
        print_run_result_error(error)

    print_run_status_line(results)
