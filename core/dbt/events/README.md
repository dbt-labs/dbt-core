# Events Module

The Events module is the implmentation for structured logging. These events represent both a programatic interface to dbt processes as well as human-readable messaging in one centralized place. The centralization allows for leveraging mypy to enforce interface invariants across all dbt events, and the distinct type layer allows for decoupling events and libraries such as loggers.

# Using the Events Module
The event module provides types that represent what is happening in dbt in `events.types`. These types are intended to represent an exhaustive list of all things happening within dbt that will need to be logged, streamed, or printed. To fire an event, `events.functions::fire_event` is the entry point to the module from everywhere in dbt.

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
