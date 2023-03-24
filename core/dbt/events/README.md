# Events Module
The Events module is responsible for communicating internal dbt structures into a consumable interface. Because the "event" classes are based entirely on protobuf definitions, the interface is really clearly defined, whether or not protobufs are used to consume it. We use Betterproto for compiling the protobuf message definitions into Python classes.

# Using the Events Module
The event module provides types that represent what is happening in dbt in `events.types`. These types are intended to represent an exhaustive list of all things happening within dbt that will need to be logged, streamed, or printed. To fire an event, `events.functions::fire_event` is the entry point to the module from everywhere in dbt.

# Logging
When events are processed via `fire_event`, nearly everything is logged. Whether or not the user has enabled the debug flag, all debug messages are still logged to the file. However, some events are particularly time consuming to construct because they return a huge amount of data. Today, the only messages in this category are cache events and are only logged if the `--log-cache-events` flag is on. This is important because these messages should not be created unless they are going to be logged, because they cause a noticable performance degredation. These events use a "fire_event_if" functions.

# Adding a New Event
* Add a new message in types.proto, and a second message with the same name + "Msg". The "Msg" message should have two fields, an "info" field of EventInfo, and a "data" field referring to the message name without "Msg"
* run the protoc compiler to update types_pb2.py:   make proto_types
* Add a wrapping class in core/dbt/event/types.py with a Level superclass  plus code and message methods
* Add the class to tests/unit/test_events.py

We have switched from using betterproto to using google protobuf, because of a lack of support for Struct fields in betterproto.

The google protobuf interface is janky and very much non-Pythonic. The "generated" classes in types_pb2.py do not resemble regular Python classes. They do not have normal constructors; they can only be constructed empty. They can be "filled" by setting fields individually or using a json_format method like ParseDict.  We have wrapped the logging events with a class (in types.py) which allows using a constructor -- keywords only, no positional parameters. 

## Required for Every Event

- a method `code`, that's unique across events
- assign a log level by using the Level mixin: `DebugLevel`, `InfoLevel`, `WarnLevel`, or `ErrorLevel`
- a message()

Example
```
class PartialParsingDeletedExposure(DebugLevel):
    def code(self):
        return "I049"

    def message(self) -> str:
        return f"Partial parsing: deleted exposure {self.unique_id}"

```


# Adapter Maintainers
To integrate existing log messages from adapters, you likely have a line of code like this in your adapter already:
```python
from dbt.logger import GLOBAL_LOGGER as logger
```

Simply change it to these two lines with your adapter's database name, and all your existing call sites will now use the new system for v1.0:
```python
from dbt.events import AdapterLogger
logger = AdapterLogger("<database name>")
# e.g. AdapterLogger("Snowflake")
```

## Compiling types.proto

After adding a new message in types.proto, execute Makefile target:

make proto_types in the repository root directory, or
`protoc -I=. --python_out=. types.proto`
in the core/dbt/events directory
