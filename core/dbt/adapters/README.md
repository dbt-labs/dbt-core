# Adapters README

The Adapters module is responsible for defining database connection methods, schema for caching, how relations are defined, and the two major types of connection types we have base and sql.

# Directories

## `base`

Defines the most primitive version of connection to a database including an abstract class inwhich macros and methods need to be defined per adapter, also defines how relations are named and made between models.

## `sql`

Defines a connection method for a database that initially inherits the above base connection and  comes with some premade methods and macros that can be overwritten as needed per adapter. (most common type of adapter.)

# Files

## `cache.py`

Defines schema for sending information back and forth between database to enusre any changes (drops, new table creations) from your dbt project are reflected in your database.

## `factory.py`
Methods of connection management between dbt and the database.

## `protocol.py`

initializes various protocl methods that can be implemented as needed per adapter.

## `reference_keys.py`

configures naming scheme for cache elements to be universal.
