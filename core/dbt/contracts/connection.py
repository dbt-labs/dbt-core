import abc
import itertools
import hashlib
from dataclasses import dataclass, field
from typing import (
    Any,
    ClassVar,
    Dict,
    Tuple,
    Iterable,
    Optional,
    List,
    Callable,
)
from dbt.exceptions import DbtInternalError
from dbt.utils import translate_aliases
from dbt.events.functions import fire_event
from dbt.events.types import NewConnectionOpening
from dbt.events.contextvars import get_node_info
from typing_extensions import Protocol
from dbt.dataclass_schema import (
    dbtClassMixin,
    StrEnum,
    ExtensibleDbtClassMixin,
    HyphenatedDbtClassMixin,
    ValidatedStringMixin,
    register_pattern,
)
from dbt.contracts.util import Replaceable


class Identifier(ValidatedStringMixin):
    ValidationRegex = r"^[A-Za-z_][A-Za-z0-9_]+$"


# we need register_pattern for jsonschema validation
register_pattern(Identifier, r"^[A-Za-z_][A-Za-z0-9_]+$")


@dataclass
class AdapterResponse(dbtClassMixin):
    _message: str
    code: Optional[str] = None
    rows_affected: Optional[int] = None

    def __str__(self):
        return self._message


class ConnectionState(StrEnum):
    INIT = "init"
    OPEN = "open"
    CLOSED = "closed"
    FAIL = "fail"


@dataclass(init=False)
class Connection(ExtensibleDbtClassMixin, Replaceable):
    type: Identifier
    name: Optional[str] = None
    state: ConnectionState = ConnectionState.INIT
    transaction_open: bool = False
    _handle: Optional[Any] = None
    _credentials: Optional[Any] = None

    def __init__(
        self,
        type: Identifier,
        name: Optional[str],
        credentials: dbtClassMixin,
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
                raise DbtInternalError(
                    "A connection's open() method attempted to read the handle value"
                ) from exc
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value


class LazyHandle:
    """The opener must be a callable that takes a Connection object and opens the
    connection, updating the handle on the Connection.
    """

    def __init__(self, opener: Callable[[Connection], Connection]):
        self.opener = opener

    def resolve(self, connection: Connection) -> Connection:
        fire_event(
            NewConnectionOpening(connection_state=connection.state, node_info=get_node_info())
        )
        return self.opener(connection)


# see https://github.com/python/mypy/issues/4717#issuecomment-373932080
# and https://github.com/python/mypy/issues/5374
# for why we have type: ignore. Maybe someday dataclasses + abstract classes
# will work.
@dataclass  # type: ignore
class Credentials(ExtensibleDbtClassMixin, Replaceable, metaclass=abc.ABCMeta):
    database: str
    schema: str
    _ALIASES: ClassVar[Dict[str, str]] = field(default={}, init=False)

    @abc.abstractproperty
    def type(self) -> str:
        raise NotImplementedError("type not implemented for base credentials class")

    @property
    def unique_field(self) -> str:
        """Hashed and included in anonymous telemetry to track adapter adoption.
        Return the field from Credentials that can uniquely identify
        one team/organization using this adapter
        """
        raise NotImplementedError("unique_field not implemented for base credentials class")

    def hashed_unique_field(self) -> str:
        return hashlib.md5(self.unique_field.encode("utf-8")).hexdigest()

    def connection_info(self, *, with_aliases: bool = False) -> Iterable[Tuple[str, Any]]:
        """Return an ordered iterator of key/value pairs for pretty-printing."""
        as_dict = self.to_dict(omit_none=False)
        connection_keys = set(self._connection_keys())
        aliases: List[str] = []
        if with_aliases:
            aliases = [k for k, v in self._ALIASES.items() if v in connection_keys]
        for key in itertools.chain(self._connection_keys(), aliases):
            if key in as_dict:
                yield key, as_dict[key]

    @abc.abstractmethod
    def _connection_keys(self) -> Tuple[str, ...]:
        raise NotImplementedError

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        data = cls.translate_aliases(data)
        return data

    @classmethod
    def translate_aliases(cls, kwargs: Dict[str, Any], recurse: bool = False) -> Dict[str, Any]:
        return translate_aliases(kwargs, cls._ALIASES, recurse)

    def __post_serialize__(self, dct):
        # no super() -- do we need it?
        if self._ALIASES:
            dct.update(
                {
                    new_name: dct[canonical_name]
                    for new_name, canonical_name in self._ALIASES.items()
                    if canonical_name in dct
                }
            )
        return dct


class UserConfigContract(Protocol):
    send_anonymous_usage_stats: bool
    use_colors: Optional[bool] = None
    partial_parse: Optional[bool] = None
    printer_width: Optional[int] = None


class HasCredentials(Protocol):
    credentials: Credentials
    profile_name: str
    user_config: UserConfigContract
    target_name: str
    threads: int

    def to_target_dict(self):
        raise NotImplementedError("to_target_dict not implemented")


DEFAULT_QUERY_COMMENT = """
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
"""


@dataclass
class QueryComment(HyphenatedDbtClassMixin):
    comment: str = DEFAULT_QUERY_COMMENT
    append: bool = False
    job_label: bool = False


class AdapterRequiredConfig(HasCredentials, Protocol):
    project_name: str
    query_comment: QueryComment
    cli_vars: Dict[str, Any]
    target_path: str
