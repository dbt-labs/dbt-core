from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple

from typing_extensions import Protocol

class _FileLike(Protocol):
    def read(self, n: int) -> bytes: ...

def unpackb(
    packed: bytes,
    file_like: Optional[_FileLike] = ...,
    read_size: int = ...,
    use_list: bool = ...,
    raw: bool = ...,
    timestamp: int = ...,
    strict_map_key: bool = ...,
    object_hook: Optional[Callable[[Dict[Any, Any]], Any]] = ...,
    object_pairs_hook: Optional[Callable[[List[Tuple[Any, Any]]], Any]] = ...,
    unicode_errors: Optional[str] = ...,
    max_buffer_size: int = ...,
    ext_hook: Callable[[int, bytes], Any] = ...,
    max_str_len: int = ...,
    max_bin_len: int = ...,
    max_array_len: int = ...,
    max_map_len: int = ...,
    max_ext_len: int = ...,
) -> Any: ...

class Unpacker:
    def __init__(
        self,
        file_like: Optional[_FileLike] = ...,
        read_size: int = ...,
        use_list: bool = ...,
        raw: bool = ...,
        timestamp: int = ...,
        strict_map_key: bool = ...,
        object_hook: Optional[Callable[[Dict[Any, Any]], Any]] = ...,
        object_pairs_hook: Optional[Callable[[List[Tuple[Any, Any]]], Any]] = ...,
        unicode_errors: Optional[str] = ...,
        max_buffer_size: int = ...,
        ext_hook: Callable[[int, bytes], Any] = ...,
        max_str_len: int = ...,
        max_bin_len: int = ...,
        max_array_len: int = ...,
        max_map_len: int = ...,
        max_ext_len: int = ...,
    ): ...
    def feed(self, next_bytes: bytes) -> None: ...
    def read_bytes(self, n: int) -> bytearray: ...
    def __iter__(self) -> Unpacker: ...
    def __next__(self) -> Any: ...
    def next(self) -> Any: ...
    def skip(self) -> None: ...
    def unpack(self) -> Any: ...
    def read_array_header(self) -> Any: ...
    def read_map_header(self) -> Any: ...
    def tell(self) -> int: ...

class Packer:
    def __init__(
        self,
        default: Optional[Callable[[Any], Any]] = ...,
        use_single_float: bool = ...,
        autoreset: bool = ...,
        use_bin_type: bool = ...,
        strict_types: bool = ...,
        datetime: bool = ...,
        unicode_errors: Optional[str] = ...,
    ): ...
    def pack(self, obj: Any) -> bytes: ...
    def pack_map_pairs(self, pairs: Any) -> bytes: ...
    def pack_array_header(self, n: int) -> bytes: ...
    def pack_map_header(self, n: int) -> bytes: ...
    def pack_ext_type(self, typecode: int, data: bytes) -> None: ...
    def bytes(self) -> bytes: ...
    def reset(self) -> None: ...
    def getbuffer(self) -> memoryview: ...
