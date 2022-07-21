from mashumaro.dialect import Dialect
from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Dict, List, Optional, Type, Union
from mashumaro.core.const import PEP_586_COMPATIBLE

if PEP_586_COMPATIBLE:
    from typing import Literal  # type: ignore
else:
    from typing_extensions import Literal  # type: ignore

TO_DICT_ADD_BY_ALIAS_FLAG: str
TO_DICT_ADD_OMIT_NONE_FLAG: str
ADD_DIALECT_SUPPORT: str

CodeGenerationOption = Literal[
    "TO_DICT_ADD_BY_ALIAS_FLAG",
    "TO_DICT_ADD_OMIT_NONE_FLAG",
    "ADD_DIALECT_SUPPORT",
]

SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class BaseConfig:
    debug: bool = ...
    code_generation_options: List[str] = ...
    serialization_strategy: Dict[Any, SerializationStrategyValueType] = ...
    aliases: Dict[str, str] = ...
    serialize_by_alias: bool = ...
    namedtuple_as_dict: bool = ...
    allow_postponed_evaluation: bool = ...
    dialect: Optional[Type[Dialect]] = ...
