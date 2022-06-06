# Adapters README

The Adapters module is responsible for defining database connection methods, schema for caching, how relations are defined, and the two major types of connection types we have base and sql.

# Directories

## `base`

Defines the most base implementation Adapters can use to build out full functionality sweet

## `sql`

Defines a sql implementation for adapters that initially inherits the above base implementation and  comes with some premade methods and macros that can be overwritten as needed per adapter. (most common type of adapter.)

# Files

## `cache.py`

Caches information from the databases to compare what is being asked of via dbt to reflect those changes in the database

## `factory.py`
Defines how we generate adapter objects

## `protocol.py`

Defines various methods to be used in dbt to database interfacing. Also can let certain dependencies like mypy what the methods used do/exist.

## `reference_keys.py`

Configures naming scheme for cache elements to be universal.
