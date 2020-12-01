import abc
import itertools
from dataclasses import dataclass, field
from typing import (
    Any, ClassVar, Dict, Tuple, Iterable, Optional, NewType, List, Callable,
)
from typing_extensions import Protocol

from hologram import JsonSchemaMixin
from hologram.helpers import (
    StrEnum, register_pattern, ExtensibleJsonSchemaMixin
)

from dbt.contracts.util import Replaceable
from dbt.exceptions import InternalException
from dbt.utils import translate_aliases

from dbt.logger import GLOBAL_LOGGER as logger


Identifier = NewType('Identifier', str)
register_pattern(Identifier, r'^[A-Za-z_][A-Za-z0-9_]+$')


class ConnectionState(StrEnum):
    INIT = 'init'
    OPEN = 'open'
    CLOSED = 'closed'
    FAIL = 'fail'


@dataclass(init=False)
class Connection(ExtensibleJsonSchemaMixin, Replaceable):
    type: Identifier
    name: Optional[str]
    state: ConnectionState = ConnectionState.INIT
    transaction_open: bool = False
    # prevent serialization
    _handle: Optional[Any] = None
    _credentials: JsonSchemaMixin = field(init=False)

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
        self.state = state
        self.credentials = credentials
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
        if isinstance(self._handle, LazyHandle):
            try:
                # this will actually change 'self._handle'.
                self._handle.resolve(self)
            except RecursionError as exc:
                raise InternalException(
                    "A connection's open() method attempted to read the "
                    "handle value"
                ) from exc
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value


class LazyHandle:
    """Opener must be a callable that takes a Connection object and opens the
    connection, updating the handle on the Connection.
    """
    def __init__(self, opener: Callable[[Connection], Connection]):
        self.opener = opener

    def resolve(self, connection: Connection) -> Connection:
        logger.debug(
            'Opening a new connection, currently in state {}'
            .format(connection.state)
        )
        return self.opener(connection)


# see https://github.com/python/mypy/issues/4717#issuecomment-373932080
# and https://github.com/python/mypy/issues/5374
# for why we have type: ignore. Maybe someday dataclasses + abstract classes
# will work.
@dataclass  # type: ignore
class Credentials(
    ExtensibleJsonSchemaMixin,
    Replaceable,
    metaclass=abc.ABCMeta
):
    database: str
    schema: str
    _ALIASES: ClassVar[Dict[str, str]] = field(default={}, init=False)

    @abc.abstractproperty
    def type(self) -> str:
        raise NotImplementedError(
            'type not implemented for base credentials class'
        )

    def connection_info(
        self, *, with_aliases: bool = False
    ) -> Iterable[Tuple[str, Any]]:
        """Return an ordered iterator of key/value pairs for pretty-printing.
        """
        as_dict = self.to_dict(omit_none=False, with_aliases=with_aliases)
        connection_keys = set(self._connection_keys())
        aliases: List[str] = []
        if with_aliases:
            aliases = [
                k for k, v in self._ALIASES.items() if v in connection_keys
            ]
        for key in itertools.chain(self._connection_keys(), aliases):
            if key in as_dict:
                yield key, as_dict[key]

    @abc.abstractmethod
    def _connection_keys(self) -> Tuple[str, ...]:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data):
        data = cls.translate_aliases(data)
        return super().from_dict(data)

    @classmethod
    def translate_aliases(
        cls, kwargs: Dict[str, Any], recurse: bool = False
    ) -> Dict[str, Any]:
        return translate_aliases(kwargs, cls._ALIASES, recurse)

    def to_dict(self, omit_none=True, validate=False, *, with_aliases=False):
        serialized = super().to_dict(omit_none=omit_none, validate=validate)
        if with_aliases:
            serialized.update({
                new_name: serialized[canonical_name]
                for new_name, canonical_name in self._ALIASES.items()
                if canonical_name in serialized
            })
        return serialized


class UserConfigContract(Protocol):
    send_anonymous_usage_stats: bool
    use_colors: Optional[bool]
    partial_parse: Optional[bool]
    printer_width: Optional[int]

    def set_values(self, cookie_dir: str) -> None:
        ...

    def to_dict(
        self, omit_none: bool = True, validate: bool = False
    ) -> Dict[str, Any]:
        ...


class HasCredentials(Protocol):
    credentials: Credentials
    profile_name: str
    config: UserConfigContract
    target_name: str
    threads: int

    def to_target_dict(self):
        raise NotImplementedError('to_target_dict not implemented')


DEFAULT_QUERY_COMMENT = '''
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}
{% else %}
  {# in the node context, the connection name is the node_id #}
  {%- do comment_dict.update(connection_name=connection_name) -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
'''


@dataclass
class QueryComment(JsonSchemaMixin):
    comment: str = DEFAULT_QUERY_COMMENT
    append: bool = False


class AdapterRequiredConfig(HasCredentials, Protocol):
    project_name: str
    query_comment: QueryComment
    cli_vars: Dict[str, Any]
    target_path: str
