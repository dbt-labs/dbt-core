
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.utils import get_materialization, NodeType
from dbt.compat import to_unicode

from colorama import Fore, Back, Style
import time
import string


def get_timestamp():
    return time.strftime("%H:%M:%S")


def color(text, color_code):
    return "{}{}{}".format(color_code, text, Style.RESET_ALL)


def green(text):
    return color(text, Fore.GREEN)


def yellow(text):
    return color(text, Fore.YELLOW)


def red(text):
    return color(text, Fore.RED)


def print_timestamped_line(msg):
    logger.info("{} | {}".format(get_timestamp(), msg))


def print_fancy_output_line(msg, status, index, total, execution_time=None):
    prefix = "{timestamp} | {index} of {total} {message}".format(
        timestamp=get_timestamp(),
        index=index,
        total=total,
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


def print_counts(flat_nodes):
    counts = {}

    for node in flat_nodes:
        t = node.get('resource_type')

        if node.get('resource_type') == NodeType.Model:
            t = '{} {}'.format(get_materialization(node), t)

        counts[t] = counts.get(t, 0) + 1

    stat_line = ", ".join(
        ["{} {}s".format(v, k) for k, v in counts.items()])

    logger.info("")
    print_timestamped_line("Running {}".format(stat_line))
    print_timestamped_line("")


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


def print_archive_result_line(result, index, total):
    model = result.node

    if result.errored:
        info = 'ERROR archiving'
        status = red(result.status)
    else:
        info = 'OK archived'
        status = green(result.status)

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

    if result.errored:
        info = 'ERROR creating'
        status = red(result.status)
    else:
        info = 'OK created'
        status = green(result.status)

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


def get_run_status_line(results):
    total = len(results)
    errored = len([r for r in results if r.errored or r.failed])
    skipped = len([r for r in results if r.skipped])
    passed = total - errored - skipped

    if errored == 0:
        penultimate_line = green('Completed successfully')
    else:
        penultimate_line = red('Completed with errors')

    return (
        "\n{status}\nDone. PASS={passed} ERROR={errored} SKIP={skipped} TOTAL={total}"
        .format(
            status=penultimate_line,
            total=total,
            passed=passed,
            errored=errored,
            skipped=skipped
        ))

