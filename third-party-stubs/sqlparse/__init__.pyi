from typing import Tuple
from . import sql
from . import tokens

def parse(sql: str) -> Tuple[sql.Statement]: ...
