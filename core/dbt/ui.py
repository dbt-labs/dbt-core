import textwrap
from typing import Dict

import colorama

from dbt.flags import get_flags

COLORS: Dict[str, str] = {
    "red": colorama.Fore.RED,
    "green": colorama.Fore.GREEN,
    "yellow": colorama.Fore.YELLOW,
    "reset_all": colorama.Style.RESET_ALL,
}


COLOR_FG_RED = COLORS["red"]
COLOR_FG_GREEN = COLORS["green"]
COLOR_FG_YELLOW = COLORS["yellow"]
COLOR_RESET_ALL = COLORS["reset_all"]


def color(text: str, color_code: str) -> str:
    if get_flags().USE_COLORS:
        return "{}{}{}".format(color_code, text, COLOR_RESET_ALL)
    else:
        return text


def printer_width() -> int:
    flags = get_flags()
    if flags.PRINTER_WIDTH:
        return flags.PRINTER_WIDTH
    return 80


def green(text: str) -> str:
    return color(text, COLOR_FG_GREEN)


def yellow(text: str) -> str:
    return color(text, COLOR_FG_YELLOW)


def red(text: str) -> str:
    return color(text, COLOR_FG_RED)


def line_wrap_message(msg: str, subtract: int = 0, dedent: bool = True, prefix: str = "") -> str:
    """
    Line wrap the given message to PRINTER_WIDTH - {subtract}. Convert double
    newlines to newlines and avoid calling textwrap.fill() on them (like
    markdown)
    """
    width = printer_width() - subtract
    if dedent:
        msg = textwrap.dedent(msg)

    if prefix:
        msg = f"{prefix}{msg}"

    # If the input had an explicit double newline, we want to preserve that
    # (we'll turn it into a single line soon). Support windows, too.
    splitter = "\r\n\r\n" if "\r\n\r\n" in msg else "\n\n"
    chunks = msg.split(splitter)
    return "\n".join(textwrap.fill(chunk, width=width, break_on_hyphens=False) for chunk in chunks)


def warning_tag(msg: str) -> str:
    return f'[{yellow("WARNING")}]: {msg}'
