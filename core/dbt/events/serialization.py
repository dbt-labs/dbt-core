from dbt.helper_types import Lazy
from mashumaro import DataClassDictMixin
from mashumaro.config import BaseConfig as MashBaseConfig
from mashumaro.types import SerializationStrategy
from typing import Dict, List


# The dbtClassMixin serialization class has a DateTime serialization strategy
# class. If a datetime ends up in an event class, we could use a similar class
# here to serialize it in our preferred format.


class ExceptionSerialization(SerializationStrategy):
    def serialize(self, value):
        out = str(value)
        return out

    def deserialize(self, value):
        return Exception(value)


class BaseExceptionSerialization(SerializationStrategy):
    def serialize(self, value):
        return str(value)

    def deserialize(self, value):
        return BaseException(value)


# This is an explicit deserializer for the type Lazy[Dict[str, List[str]]]
# mashumaro does not support composing serialization strategies, so all
# future uses of Lazy will need to register a unique serialization class like this one.
class LazySerialization1(SerializationStrategy):
    def serialize(self, value) -> Dict[str, List[str]]:
        return value.force()

    # we _can_ deserialize into a lazy value, but that defers running the deserialization
    # function till the value is used which can raise errors at very unexpected times.
    # It's best practice to do strict deserialization unless you're in a very special case.
    def deserialize(self, value):
        raise Exception("Don't deserialize into a Lazy value. Try just using the value itself.")


# This class is the equivalent of dbtClassMixin that's used for serialization
# in other parts of the code. That class did extra things which we didn't want
# to use for events, so this class is a simpler version of dbtClassMixin.
class EventSerialization(DataClassDictMixin):

    # This is where we register serializtion strategies per type.
    class Config(MashBaseConfig):
        serialization_strategy = {
            Exception: ExceptionSerialization(),
            BaseException: ExceptionSerialization(),
            Lazy[Dict[str, List[str]]]: LazySerialization1(),
        }
