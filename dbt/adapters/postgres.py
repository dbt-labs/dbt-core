import copy
import psycopg2
import re
import time
import yaml

from contextlib import contextmanager

import dbt.flags as flags

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Schema, READ_PERMISSION_DENIED_ERROR

connection_cache = {}

RELATION_PERMISSION_DENIED_MESSAGE = """
The user '{user}' does not have sufficient permissions to create the model
'{model}' in the schema '{schema}'. Please adjust the permissions of the
'{user}' user on the '{schema}' schema. With a superuser account, execute the
following commands, then re-run dbt.

grant usage, create on schema "{schema}" to "{user}";
grant select, insert, delete on all tables in schema "{schema}" to "{user}";"""

RELATION_NOT_OWNER_MESSAGE = """
The user '{user}' does not have sufficient permissions to drop the model
'{model}' in the schema '{schema}'. This is likely because the relation was
created by a different user. Either delete the model "{schema}"."{model}"
manually, or adjust the permissions of the '{user}' user in the '{schema}'
schema."""


@contextmanager
def exception_handler(connection, cursor, model_name):
    handle = connection.get('handle')
    schema = connection.get('credentials', {}).get('schema')

    try:
        yield
    except psycopg2.ProgrammingError as e:
        handle.rollback()
        error_data = {"model": model_name,
                      "schema": schema,
                      "user": connection.get('credentials', {}).get('user')}
        if 'must be owner of relation' in e.diag.message_primary:
            raise RuntimeError(
                RELATION_NOT_OWNER_MESSAGE.format(**error_data))
        elif "permission denied for" in e.diag.message_primary:
            raise RuntimeError(
                RELATION_PERMISSION_DENIED_MESSAGE.format(**error_data))
        else:
            raise e
    except Exception as e:
        handle.rollback()
        logger.exception("Error running SQL: %s", sql)
        logger.debug("rolling back connection")
        raise e
    finally:
        cursor.close()


class PostgresAdapter:

    @classmethod
    def acquire_connection(cls, profile):

        # profile requires some marshalling right now because it includes a
        # wee bit of global config.
        # TODO remove this
        credentials = copy.deepcopy(profile)

        credentials.pop('type', None)
        credentials.pop('threads', None)

        result = {
            'type': 'postgres',
            'state': 'init',
            'handle': None,
            'credentials': credentials
        }

        logger.debug('Acquiring postgres connection')

        if flags.STRICT_MODE:
            validate_connection(result)

        return cls.open_connection(result)

    @staticmethod
    def hash_profile(profile):
        return ("{}--{}--{}--{}".format(
            profile.get('host'),
            profile.get('dbname'),
            profile.get('schema'),
            profile.get('user'),
        ))

    @classmethod
    def get_connection(cls, profile):
        profile_hash = cls.hash_profile(profile)

        if connection_cache.get(profile_hash):
            connection = connection_cache.get(profile_hash)
            return connection

        connection = cls.acquire_connection(profile)
        connection_cache[profile_hash] = connection

        return connection

    @staticmethod
    def get_connection_spec(connection):
        credentials = connection.get('credentials')

        return ("dbname='{}' user='{}' host='{}' password='{}' port='{}' "
                "connect_timeout=10".format(
                    credentials.get('dbname'),
                    credentials.get('user'),
                    credentials.get('host'),
                    credentials.get('pass'),
                    credentials.get('port'),
                ))

    @classmethod
    def open_connection(cls, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            handle = psycopg2.connect(cls.get_connection_spec(connection))

            result['handle'] = handle
            result['state'] = 'open'
        except psycopg2.Error as e:
            logger.debug("Got an error when attempting to open a postgres "
                         "connection: '{}'"
                         .format(e))

            result['handle'] = None
            result['state'] = 'fail'

        return result

    @classmethod
    def create_schema(cls, profile, schema):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        sql = ('create_schema_if_not_exists "{schema}"'
               .format(schema=schema))

        handle, status = cls.add_query_to_transaction(
            sql, connection, model_name)

    @staticmethod
    def create_table():
        pass

    @classmethod
    def drop(cls, profile, relation, relation_type, model_name=None):
        if relation_type == 'view':
            return cls.drop_view(profile, relation, model_name)
        elif relation_type == 'table':
            return cls.drop_table(profile, relation, model_name)
        else:
            raise RuntimeError(
                "Invalid relation_type '{}'"
                .format(relation_type))

    @classmethod
    def drop_view(cls, profile, view, model_name):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('drop view if exists "{schema}"."{view}" cascade'
               .format(
                   schema=schema,
                   view=view))

        handle, status = cls.add_query_to_transaction(
            sql, connection, model_name)

    @classmethod
    def drop_table(cls, profile, table, model_name):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('drop table if exists "{schema}"."{table}" cascade'
               .format(
                   schema=schema,
                   table=table))

        handle, status = cls.add_query_to_transaction(
            sql, connection, model_name)

    @classmethod
    def truncate(cls, profile, table, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('truncate table "{schema}"."{table}"'
               .format(
                   schema=schema,
                   table=table))

        handle, status = cls.add_query_to_transaction(
            sql, connection, model_name)

    @classmethod
    def rename(cls, profile, from_name, to_name, model_name=None):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('alter table "{schema}"."{from_name}" rename to "{to_name}"'
               .format(
                   schema=schema,
                   from_name=from_name,
                   to_name=to_name))

        handle, status = cls.add_query_to_transaction(
            sql, connection, model_name)

    @classmethod
    def execute_model(cls, project, target, model):
        schema_helper = Schema(project, target)
        parts = re.split(r'-- (DBT_OPERATION .*)', model.compiled_contents)
        profile = project.run_environment()
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        status = 'None'
        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                func_map = {
                    'expand_column_types_if_needed':
                    lambda kwargs: schema_helper.expand_column_types_if_needed(
                        **kwargs)
                }

                func_map[function](kwargs)
            else:
                handle, status = cls.add_query_to_transaction(
                    part, connection, model.name)

        handle.commit()
        return status

    @classmethod
    def commit(cls, profile):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        handle = connection.get('handle')
        handle.commit()

    @staticmethod
    def add_query_to_transaction(sql, connection, model_name=None):
        handle = connection.get('handle')
        cursor = handle.cursor()

        with exception_handler(connection, cursor, model_name):
            logger.debug("SQL: %s", sql)
            pre = time.time()
            cursor.execute(sql)
            post = time.time()
            logger.debug(
                "SQL status: %s in %0.2f seconds",
                cursor.statusmessage, post-pre)
            return handle, cursor.statusmessage
