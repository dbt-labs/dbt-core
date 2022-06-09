# Adapters README

The Adapters module is responsible for defining database connection methods, caching information from databases, how relations are defined, and the two major connection types we have - base and sql.

# Directories

## `base`

Defines the base implementation Adapters can use to build out full functionality.

## `sql`

Defines a sql implementation for adapters that initially inherits the above base implementation and  comes with some premade methods and macros that can be overwritten as needed per adapter. (most common type of adapter.)

# Files

## `cache.py`

Cached information from the database.

## `factory.py`
Defines how we generate adapter objects

## `protocol.py`

Defines various interfaces for various adapter objects. Helps mypy correctly resolve methods.

## `reference_keys.py`

Configures naming scheme for cache elements to be universal.
