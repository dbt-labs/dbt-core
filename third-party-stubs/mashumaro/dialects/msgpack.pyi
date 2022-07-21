from mashumaro.dialect import Dialect
from typing import Any

class MessagePackDialect(Dialect):
    serialization_strategy: Any = ...
