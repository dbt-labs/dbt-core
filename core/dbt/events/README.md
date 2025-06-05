# Events Module
The Events module is responsible for communicating internal dbt structures into a consumable interface. Because the "event" classes are based entirely on protobuf definitions, the interface is really clearly defined, whether or not protobufs are used to consume it. We use protoc for compiling the protobuf message definitions into Python classes.

# Using the Events Module
The event module provides types that represent what is happening in dbt in `events.types`. These types are intended to represent an exhaustive list of all things happening within dbt that will need to be logged, streamed, or printed. To fire an event, `common.events.functions::fire_event` is the entry point to the module from everywhere in dbt.

# Logging
When events are processed via `fire_event`, nearly everything is logged. Whether or not the user has enabled the debug flag, all debug messages are still logged to the file. However, some events are particularly time consuming to construct because they return a huge amount of data. Today, the only messages in this category are cache events and are only logged if the `--log-cache-events` flag is on. This is important because these messages should not be created unless they are going to be logged, because they cause a noticable performance degredation. These events use a "fire_event_if" functions.

# Adding a New Event
All protos have been moved into the central protos repository. To edit an event proto, edit https://github.com/dbt-labs/proto-python-public or open an issue on that repository.

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


## Compiling core_types.proto

After adding a new message in `core_types.proto`, either:
- In the repository root directory: `make core_proto_types`
- In the `core/dbt/events` directory: `protoc -I=. --python_out=. types.proto`
