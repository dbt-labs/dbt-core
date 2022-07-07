from mashumaro.dialect import Dialect
from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Dict, List, Optional, Type, Union

TO_DICT_ADD_BY_ALIAS_FLAG: str
TO_DICT_ADD_OMIT_NONE_FLAG: str
ADD_DIALECT_SUPPORT: str
SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class BaseConfig:
    debug: bool
    code_generation_options: List[Any]
    serialization_strategy: Dict[Any, SerializationStrategyValueType]
    aliases: Dict[str, str]
    serialize_by_alias: bool
    namedtuple_as_dict: bool
    allow_postponed_evaluation: bool
    dialect: Optional[Type[Dialect]]
