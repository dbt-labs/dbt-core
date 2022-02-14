from typing import Any, Mapping, Dict, Optional

class DataClassDictMixin:
    def __init_subclass__(cls, **kwargs: Any) -> None: ...
    def __pre_serialize__(self) -> Any: ...
    def __post_serialize__(self, dct: Mapping) -> Any: ...
    @classmethod
    def __pre_deserialize__(cls: Any, dct: Mapping) -> Any: ...
    # This is absolutely totally wrong. This is *not* the signature of the Mashumaro to_dict
    # But mypy insists that the DataClassDictMixin to_dict and the JsonSchemaMixin to_dict
    # must have the same signatures now that we have an 'omit_none' flag on the Mashumaro to_dict.
    # There is no 'validate = False' in Mashumaro.
    # Could not find a way to tell mypy to ignore it.
    def to_dict(self, omit_none=False, validate=False) -> dict: ...
    @classmethod
    def from_dict(
        cls,
        d: Mapping,
        use_bytes: bool = False,
        use_enum: bool = False,
        use_datetime: bool = False,
    ) -> Any: ...
