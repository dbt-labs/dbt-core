from typing import Any, Callable, Optional, Union


def field_options(
    serialize: Optional[Callable[[Any], Any]] = None,
    deserialize: Optional[Union[str, Callable[[Any], Any]]] = None,
):
    return {"serialize": serialize, "deserialize": deserialize}


__all__ = ["field_options"]
