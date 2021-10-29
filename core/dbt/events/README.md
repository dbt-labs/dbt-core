# Events Module

The Events module is the implmentation for structured logging. These events represent both a programatic interface to dbt processes as well as human-readable messaging in one centralized place. The centralization allows for leveraging mypy to enforce interface invariants across all dbt events, and the distinct type layer allows for decoupling events and libraries such as loggers.

# Using the Events Module
The event module provides types that represent what is happening in dbt in `events.types`. These types are intended to represent an exhaustive list of all things happening within dbt that will need to be logged, streamed, or printed. To fire an event, `events.functions::fire_event` is the entry point to the module from everywhere in dbt.

# Adding a New Event
In `events.types` add a new class that represents the new event. This may be a simple class with no values, or it may be a dataclass with some values to construct downstream messaging. Only include the data necessary to construct this message within this class. You must extend all destinations (e.g. - if your log message belongs on the cli, extend `CliEventABC`) as well as the loglevel this event belongs to.
