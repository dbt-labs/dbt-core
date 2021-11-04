import dbt.logger as logger  # type: ignore # TODO eventually remove dependency on this logger
from dbt import ui
from typing import Optional


def format_fancy_output_line(
        msg: str, status: str, index: Optional[int],
        total: Optional[int], execution_time: Optional[float] = None,
        truncate: bool = False
) -> str:
    if index is None or total is None:
        progress = ''
    else:
        progress = '{} of {} '.format(index, total)
    # TODO: remove this formatting once we rip out all the old logger
    prefix = "{timestamp} | {progress}{message}".format(
        timestamp=logger.get_timestamp(),
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

    return output
