# Events Module

The Events module is responsible for communicating internal dbt structures into a consumable interface. Right now, the events module is exclusively used for structured logging, but in the future could grow to include other user-facing components such as exceptions. These events represent both a programatic interface to dbt processes as well as human-readable messaging in one centralized place. The centralization allows for leveraging mypy to enforce interface invariants across all dbt events, and the distinct type layer allows for decoupling events and libraries such as loggers.

# Using the Events Module
The event module provides types that represent what is happening in dbt in `events.types`. These types are intended to represent an exhaustive list of all things happening within dbt that will need to be logged, streamed, or printed. To fire an event, `events.functions::fire_event` is the entry point to the module from everywhere in dbt.

# Logging
When events are processed via `fire_event`, nearly everything is logged. Whether or not the user has enabled the debug flag, all debug messages are still logged to the file. However, some events are particularly time consuming to construct because they return a huge amount of data. Today, the only messages in this category are cache events and are only logged if the `--log-cache-events` flag is on. This is important because these messages should not be created unless they are going to be logged, because they cause a noticable performance degredation. We achieve this by making the event class explicitly use lazy values for the expensive ones so they are not computed until the moment they are required. This is done with the data type `core/dbt/helper_types.py::Lazy` which includes usage documentation.

Example:
```
@dataclass
class DumpBeforeAddGraph(DebugLevel, Cache):
    dump: Lazy[Dict[str, List[str]]]
    code: str = "E031"

    def message(self) -> str:
        return f"before adding : {self.dump.force()}"
```


# Adding a New Event
In `events.types` add a new class that represents the new event. All events must be a dataclass with, at minimum, a code.  You may also include some other values to construct downstream messaging. Only include the data necessary to construct this message within this class. You must extend all destinations (e.g. - if your log message belongs on the cli, extend `Cli`) as well as the loglevel this event belongs to.  This system has been designed to take full advantage of mypy so running it will catch anything you may miss.

## Required for Every Event

- a string attribute `code`, that's unique across events
- assign a log level by extending `DebugLevel`, `InfoLevel`, `WarnLevel`, or `ErrorLevel`
- a message()
- extend `File` and/or `Cli` based on where it should output

Example
```
@dataclass
class PartialParsingDeletedExposure(DebugLevel, Cli, File):
    unique_id: str
    code: str = "I049"

    def message(self) -> str:
        return f"Partial parsing: deleted exposure {self.unique_id}"

```

## Optional (based on your event)

- Events associated with node status changes must be extended with `NodeInfo` which contains a node_info attribute


All values other than `code` and `node_info` will be included in the `data` node of the json log output.

Once your event has been added, add a dummy call to your new event at the bottom of `types.py` and also add your new Event to the list `sample_values` in `test/unit/test_events.py'.

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
