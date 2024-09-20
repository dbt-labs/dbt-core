from mashumaro.core.const import Sentinel
from mashumaro.types import SerializationStrategy
from typing import Callable, Dict, Union
from typing_extensions import Literal

SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class Dialect:
    serialization_strategy: Dict[str, SerializationStrategyValueType]
    omit_none: Union[bool, Sentinel.MISSING]
