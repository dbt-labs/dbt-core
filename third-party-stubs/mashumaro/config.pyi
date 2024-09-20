from mashumaro.core.const import Sentinel
from mashumaro.dialect import Dialect
from mashumaro.types import Discriminator, SerializationStrategy
from typing import Any, Callable, Dict, List, Optional, Type, Union
from typing_extensions import Literal

TO_DICT_ADD_BY_ALIAS_FLAG: str
TO_DICT_ADD_OMIT_NONE_FLAG: str
ADD_DIALECT_SUPPORT: str
ADD_SERIALIZATION_CONTEXT: str
SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class BaseConfig:
    debug: bool
    code_generation_options: List[str]
    serialization_strategy: Dict[Any, SerializationStrategyValueType]
    aliases: Dict[str, str]
    serialize_by_alias: bool
    namedtuple_as_dict: bool
    allow_postponed_evaluation: bool
    dialect: Optional[Type[Dialect]]
    omit_none: Union[bool, Sentinel.MISSING]
    orjson_options: Optional[int]
    json_schema: Dict[str, Any]
    discriminator: Optional[Discriminator]
    lazy_compilation: bool
