import decimal
from typing import TypeVar, Generic

TV = TypeVar("TV")

class SerializableEncoder(Generic[TV]):
    @classmethod
    def _serialize(cls, value):
        raise NotImplementedError

    @classmethod
    def _deserialize(cls, value):
        raise NotImplementedError

class SerializableType:
    def _serialize(self):
        raise NotImplementedError

    @classmethod
    def _deserialize(cls, value):
        raise NotImplementedError


class SerializationStrategy:
    def _serialize(self, value):
        raise NotImplementedError

    def _deserialize(self, value):
        raise NotImplementedError


class RoundedDecimal(SerializationStrategy):
    def __init__(self, places=None, rounding=None):
        if places is not None:
            self.exp = decimal.Decimal((0, (1,), -places))
        else:
            self.exp = None
        self.rounding = rounding

    def _serialize(self, value) -> str:
        if self.exp:
            if self.rounding:
                return str(value.quantize(self.exp, rounding=self.rounding))
            else:
                return str(value.quantize(self.exp))
        else:
            return str(value)

    def _deserialize(self, value: str) -> decimal.Decimal:
        return decimal.Decimal(str(value))


__all__ = [
    "SerializableType",
    "SerializationStrategy",
    "RoundedDecimal",
]
