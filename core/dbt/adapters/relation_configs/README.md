# RelationConfig
This package serves as an initial abstraction for managing the inspection of existing relations and determining
changes on those relations. It arose from the materialized view work and is currently only supporting 
materialized views for Postgres and Redshift as well as dynamic tables for Snowflake. There are three main
classes in this package.

## RelationConfigBase
This is a very small class that only has a `from_dict()` method and a default `NotImplementedError()`. At some
point this could be replaced by a more robust framework, like `mashumaro` or `pydantic`.

## RelationConfigChange
This class inherits from `RelationConfigBase` ; however, this can be thought of as a separate class. The subclassing
merely points to the idea that both classes would likely inherit from the same class in a `mashumaro` or
`pydantic` implementation. This class is much more restricted in attribution. It should really only
ever need an `action` and a `context`. This can be though of as being analogous to a web request. You need to
know what you're doing (`action`: 'create' = GET, 'drop' = DELETE, etc.) and the information (`context`) needed
to make the change. In our scenarios, the context tends to be an instance of `RelationConfigBase` corresponding
to the new state.

## RelationConfigValidationMixin
This mixin provides optional validation mechanics that can be applied to either `RelationConfigBase` or
`RelationConfigChange` subclasses. A validation rule is a combination of a `validation_check`, something
that should evaluate to `True`, and an optional `validation_error`, an instance of `DbtRuntimeError`
that should be raised in the event the `validation_check` fails. While optional, it's recommended that
the `validation_error` be provided for clearer transparency to the end user.
