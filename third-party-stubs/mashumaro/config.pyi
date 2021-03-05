from mashumaro.types import SerializationStrategy as SerializationStrategy
from typing import Any, Callable, Dict, List, Union

TO_DICT_ADD_OMIT_NONE_FLAG: str
SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class BaseConfig:
    debug: bool = ...
    code_generation_options: List[str] = ...
    serialization_strategy: Dict[Any, SerializationStrategyValueType] = ...
