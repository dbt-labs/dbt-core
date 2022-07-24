from mashumaro.types import SerializationStrategy
from typing import Any, Callable, Dict, Union

SerializationStrategyValueType = Union[SerializationStrategy, Dict[str, Union[str, Callable]]]

class Dialect:
    serialization_strategy: Dict[Any, SerializationStrategyValueType] = ...
