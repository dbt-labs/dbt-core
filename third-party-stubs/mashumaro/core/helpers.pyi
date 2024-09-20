import datetime
from _typeshed import Incomplete

UTC_OFFSET_PATTERN: str

def parse_timezone(s: str) -> datetime.timezone: ...

class ConfigValue:
    name: Incomplete
    def __init__(self, name: str) -> None: ...
