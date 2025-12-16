import argparse
import tomllib

import adbc_driver_manager
import adbc_driver_manager.dbapi
import readline
import pyarrow as pa

DESCRIPTION = """
ADBC Python REPL

This is a program that allows you to interact with ADBC through a simple shell-like REPL.

COMMANDS
    exec <SQL> 
        run a SQL statement against the warehouse

    get_objects [<identifier>] 
        get all objects in the, optionally matching the given identifier

    get_schema <identifier>
        get the schema of the table with the given identifier

    exit
        exit the REPL

EXAMPLES
    # Execute a SELECT 1
    exec SELECT 1

    # List all objects (tables, views etc) in `my_project.my_schema`
    get_objects my_project.my_schema

    # Get the schema of `my_project.my_schema.my_table`
    get_schema my_project.my_table
"""


def parse_args():
    parser = argparse.ArgumentParser(DESCRIPTION)
    parser.add_argument(
        "--config", default="adbc_conf.toml", help="Path to TOML config file"
    )
    parser.add_argument(
        "--profile", default="default", help="Profile name in config file"
    )
    return parser.parse_args()


args = parse_args()

# Read config file and profile
with open(args.config, "rb") as f:
    config = tomllib.load(f)

if args.profile not in config:
    raise ValueError(f"Profile '{args.profile}' not found in config file")

profile_config = config[args.profile]

driver_path = profile_config.pop("path")

if not driver_path:
    raise ValueError(f"'path' must be set for profile '{args.profile}'")


def parse_table_identifier(s: str) -> tuple[str | None, str | None, str]:
    parts = s.split(".")
    while len(parts) < 3:
        parts = [None] + parts

    return parts[0], parts[1], parts[2]


with adbc_driver_manager.AdbcDatabase(driver=driver_path, **profile_config) as db:
    print("DRIVER LOADED: ", driver_path)
    with adbc_driver_manager.AdbcConnection(db) as conn:
        dbapi = adbc_driver_manager.dbapi.Connection(db, conn)
        print("CONNECTION ESTABLISHED")

        command = ""
        while True:
            try:
                split = input("> ").split(" ", 1)
                if len(split) == 1:
                    command = split[0]
                    args = ""
                else:
                    command, args = split
            except:
                print("Invalid command. Please use '<command> <args>'")
                continue

            try:
                match command:
                    case "exec":
                        with dbapi.cursor() as cursor:
                            cursor.execute(args)
                            table = cursor.fetch_arrow_table()
                            print(table.to_string(show_metadata=True))

                    case "get_objects":
                        parts = parse_table_identifier(args)
                        objects = dbapi.adbc_get_objects(
                            table_name_filter=parts[2],
                            db_schema_filter=parts[1],
                            catalog_filter=parts[0],
                        ).read_all()
                        print(objects.to_string(show_metadata=True))

                    case "get_schema":
                        parts = parse_table_identifier(args)
                        schema = dbapi.adbc_get_table_schema(
                            table_name=parts[2],
                            db_schema_filter=parts[1],
                            catalog_filter=parts[0],
                        )
                        print(schema)

                    case "exit":
                        break

                    case _:
                        print("Unrecognized command:", command)
            except Exception as err:
                print("Error from ADBC", err)

            print("----------")
