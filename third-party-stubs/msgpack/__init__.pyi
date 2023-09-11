from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple
from msgpack.exceptions import (
    BufferFull,
    ExtraData,
    FormatError,
    OutOfData,
    PackException,
    PackOverflowError,
    PackValueError,
    StackError,
    UnpackException,
    UnpackValueError,
)
from typing_extensions import Protocol
from msgpack.fallback import Packer, Unpacker, unpackb
from msgpack import exceptions
from msgpack.ext import ExtType
from msgpack import ext

class _Stream(Protocol):
    def read(self) -> bytes: ...

class _FileLike(Protocol):
    def read(self, n: int) -> bytes: ...

def pack(
    o: Any,
    stream: _Stream,
    default: Optional[Callable[[Any], Any]] = ...,
    use_single_float: bool = ...,
    autoreset: bool = ...,
    use_bin_type: bool = ...,
    strict_types: bool = ...,
    datetime: bool = ...,
    unicode_errors: Optional[str] = ...,
) -> None: ...
def packb(
    o: Any,
    default: Optional[Callable[[Any], Any]] = ...,
    use_single_float: bool = ...,
    autoreset: bool = ...,
    use_bin_type: bool = ...,
    strict_types: bool = ...,
    datetime: bool = ...,
    unicode_errors: Optional[str] = ...,
) -> bytes: ...
def unpack(
    stream: _Stream,
    file_like: Optional[_FileLike] = ...,
    read_size: int = ...,
    use_list: bool = ...,
    raw: bool = ...,
    timestamp: int = ...,
    strict_map_key: bool = ...,
    object_hook: Optional[Callable[[Dict[Any, Any]], Any]] = ...,
    object_pairs_hook: Optional[Callable[[List[Tuple[Any, Any]]], Any]] = ...,
    list_hook: Optional[Callable[[List[Any]], Any]] = ...,
    unicode_errors: Optional[str] = ...,
    max_buffer_size: int = ...,
    ext_hook: Callable[[int, bytes], Any] = ...,
    max_str_len: int = ...,
    max_bin_len: int = ...,
    max_array_len: int = ...,
    max_map_len: int = ...,
    max_ext_len: int = ...,
) -> Any: ...

load = unpack
loads = unpackb

dump = pack
dumps = packb

__all__ = [
    "BufferFull",
    "ExtType",
    "ExtraData",
    "FormatError",
    "OutOfData",
    "PackException",
    "PackOverflowError",
    "PackValueError",
    "Packer",
    "StackError",
    "UnpackException",
    "UnpackValueError",
    "Unpacker",
    "dump",
    "dumps",
    "exceptions",
    "ext",
    "load",
    "loads",
    "pack",
    "packb",
    "unpack",
    "unpackb",
]
