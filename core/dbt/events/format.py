from dbt import ui
from dbt.node_types import NodeType
from typing import Optional, Union


def format_fancy_output_line(
    msg: str,
    status: str,
    index: Optional[int],
    total: Optional[int],
    execution_time: Optional[float] = None,
    truncate: bool = False,
) -> str:
    if index is None or total is None:
        progress = ""
    else:
        progress = "{} of {} ".format(index, total)
    prefix = "{progress}{message} ".format(progress=progress, message=msg)

    truncate_width = ui.printer_width() - 3
    justified = prefix.ljust(ui.printer_width(), ".")
    if truncate and len(justified) > truncate_width:
        justified = justified[:truncate_width] + "..."

    if execution_time is None:
        status_time = ""
    else:
        status_time = " in {execution_time:0.2f}s".format(execution_time=execution_time)

    output = "{justified} [{status}{status_time}]".format(
        justified=justified, status=status, status_time=status_time
    )

    return output


def _pluralize(string: Union[str, NodeType]) -> str:
    try:
        convert = NodeType(string)
    except ValueError:
        return f"{string}s"
    else:
        return convert.pluralize()


def pluralize(count, string: Union[str, NodeType]):
    pluralized: str = str(string)
    if count != 1:
        pluralized = _pluralize(string)
    return f"{count} {pluralized}"
