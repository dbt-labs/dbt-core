from hologram.helpers import StrEnum, NewPatternType, ExtensibleJsonSchemaMixin
from hologram import JsonSchemaMixin
from dbt.contracts.util import Replaceable

from dataclasses import dataclass
from typing import Any, Optional


Identifier = NewPatternType('Identifier', r'^[A-Za-z_][A-Za-z0-9_]+$')


class ConnectionState(StrEnum):
    INIT = 'init'
    OPEN = 'open'
    CLOSED = 'closed'
    FAIL = 'fail'


@dataclass(init=False)
class Connection(ExtensibleJsonSchemaMixin, Replaceable):
    type: Identifier
    name: Optional[str]
    _credentials: JsonSchemaMixin = None  # underscore to prevent serialization
    state: ConnectionState = ConnectionState.INIT
    transaction_open: bool = False
    _handle: Optional[Any] = None  # underscore to prevent serialization

    def __init__(
        self,
        type: Identifier,
        name: Optional[str],
        credentials: JsonSchemaMixin,
        state: ConnectionState = ConnectionState.INIT,
        transaction_open: bool = False,
        handle: Optional[Any] = None,
    ) -> None:
        self.type = type
        self.name = name
        self.credentials = credentials
        self.state = state
        self.transaction_open = transaction_open
        self.handle = handle

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        self._credentials = value

    @property
    def handle(self):
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value
