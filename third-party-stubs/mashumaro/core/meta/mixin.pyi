from mashumaro.dialect import Dialect
from typing import Any, Dict, Optional, Tuple, Type

def compile_mixin_packer(
    cls,
    format_name: str = ...,
    dialect: Optional[Type[Dialect]] = ...,
    encoder: Any = ...,
    encoder_kwargs: Optional[Dict[str, Dict[str, Tuple[str, Any]]]] = ...,
) -> None: ...
def compile_mixin_unpacker(
    cls, format_name: str = ..., dialect: Optional[Type[Dialect]] = ..., decoder: Any = ...
) -> None: ...
