# Structured Logging Arch

## Context
Consumers of dbt have been relying on log parsing well before this change. However, our logs were never optimized for programatic consumption, nor were logs treated like a formal interface between dbt and users. dbt's logging strategy was changed explicitly to address these two realities.

### Options
#### How to structure the data
- Using a library like structlog to represent log data with structural types like dictionaries. This would allow us to easily add data to a log event's context at each call site and have structlog do all the string formatting and io work.
- Creating our own nominal type layer that describes each event in source. This allows event fields to be enforced statically via mypy accross all call sites.

#### How to output the data
- Using structlog to output log lines regardless of if we used it to represent the data. The defaults for structlog are good, and it handles json vs text and formatting for us.
- Using the std lib logger to log our messages more manually. Easy to use, but does far less for us.

## Decision
#### How to structure the data
We decided to go with a custom nominal type layer even though this was going to be more work. This type layer centralizes our assumptions about what data each log event contains, and allows us to use mypy to enforce these centralized assumptions acrosss the codebase. This is all for the purpose for treating logs like a formal interface between dbt and users. Here are two concrete, practical examples of how this pattern is used:

    1. On the abstract superclass of all events, there are abstract methods and fields that each concrete class must implement such as `level_tag()` and `code`. If you make a new concrete event type without those, mypy will fail and tell you that you need them, preventing lost log lines, and json log events without a computer-friendly code.

    2. On each concrete event, the fields we need to construct the message are explicitly in the source of the class. At every call site if you construct an event without all the necessary data, mypy will fail and tell you which fields you are missing.

Using mypy to enforce these assumptions is a step better than testing becacuse we do not need to write tests to run through every branch of dbt that it could take. Because it is checked statically on every file, mypy will give us these guarantees as long as it is configured to run everywhere.

#### How to output the data
We decided to use the std lib logger because it was far more difficult than we expected to get to structlog to work properly. Documentation was lacking, and reading the source code wasn't a quick way to learn. The std lib logger was used mostly out of a necessity, and because many of the pleasantries you get from using a log library we had already chosen to do explicitly with functions in our nominal typing layer. Swapping out the std lib logger in the future should be an easy task should we choose to do it.

## Status
Completed

## Consequences
Adding a new log event is more cumbersome than it was previously: instead of writing the message at the log callsite, you must create a new concrete class in the event types. This is more opaque for new contributors. The json serialization approach we are using via `asdict` is fragile and unoptimized and should be replaced.

All user-facing log messages now live in one file which makes the job of conforming them much simpler. Because they are all nominally typed separately, it opens up the possibility to have log documentation generated from the type hints as well as outputting our logs in multiple human languages if we want to translate our messages.
