import copy
import psycopg2
import re
import time
import yaml

import dbt.flags as flags

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Schema, READ_PERMISSION_DENIED_ERROR

connection_cache = {}


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

    @staticmethod
    def create_table():
        pass

    @classmethod
    def drop(cls, profile, relation, relation_type):
        if relation_type == 'view':
            return cls.drop_view(profile, relation)
        elif relation_type == 'table':
            return cls.drop_table(profile, relation)
        else:
            raise RuntimeError(
                "Invalid relation_type '{}'"
                .format(relation_type))

    @classmethod
    def drop_view(cls, profile, view):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('drop view if exists "{schema}"."{view}" cascade'
               .format(
                   schema=schema,
                   view=view))

        handle, status = cls.add_query_to_transaction(sql, connection)

    @classmethod
    def drop_table(cls, profile, table):
        connection = cls.get_connection(profile)

        if flags.STRICT_MODE:
            validate_connection(connection)

        schema = connection.get('credentials', {}).get('schema')

        sql = ('drop table if exists "{schema}"."{table}" cascade'
               .format(
                   schema=schema,
                   table=table))

        handle, status = cls.add_query_to_transaction(sql, connection)

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
                try:
                    handle, status = cls.add_query_to_transaction(
                        part, connection)
                except psycopg2.ProgrammingError as e:
                    if "permission denied for" in e.diag.message_primary:
                        raise RuntimeError(READ_PERMISSION_DENIED_ERROR.format(
                            model=model.name,
                            error=str(e).strip(),
                            user=target.user,
                        ))
                    else:
                        raise

        handle.commit()
        return status

    @staticmethod
    def add_query_to_transaction(sql, connection):
        handle = connection.get('handle')
        cursor = handle.cursor()

        try:
            logger.debug("SQL: %s", sql)
            pre = time.time()
            cursor.execute(sql)
            post = time.time()
            logger.debug(
                "SQL status: %s in %0.2f seconds",
                cursor.statusmessage, post-pre)
            return handle, cursor.statusmessage
        except Exception as e:
            handle.rollback()
            logger.exception("Error running SQL: %s", sql)
            logger.debug("rolling back connection")
            raise e
        finally:
            cursor.close()
