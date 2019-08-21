from typing import Dict

import colorama

COLORS: Dict[str, str] = {
    'red': colorama.Fore.RED,
    'green': colorama.Fore.GREEN,
    'yellow': colorama.Fore.YELLOW,
    'reset_all': colorama.Style.RESET_ALL
}
