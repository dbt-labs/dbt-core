import psycopg2
import re
import yaml

from dbt.contracts.connection import validate_connection
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.schema import Schema, READ_PERMISSION_DENIED_ERROR

class PostgresAdapter:

    @staticmethod
    def acquire_connection(cfg, profile):
        result = {
            'type': 'postgres',
            'state': 'init',
            'handle': None
        }

        logger.debug('Acquiring postgres connection')

        if cfg.get('STRICT_MODE', False):
            logger.debug('Strict mode on, validating connection')
            validate_connection(result)

        return PostgresAdapter.open_connection(cfg, profile, result)

    @staticmethod
    def get_connection():
        pass

    @staticmethod
    def create_table():
        pass

    @staticmethod
    def drop_table():
        pass

    @staticmethod
    def execute_model(cfg, project, target, model):
        schema_helper = Schema(project, target)
        parts = re.split(r'-- (DBT_OPERATION .*)', model.compiled_contents)
        handle = None

        status = 'None'
        for i, part in enumerate(parts):
            matches = re.match(r'^DBT_OPERATION ({.*})$', part)
            if matches is not None:
                instruction_string = matches.groups()[0]
                instruction = yaml.safe_load(instruction_string)
                function = instruction['function']
                kwargs = instruction['args']

                func_map = {
                    'expand_column_types_if_needed': \
                    lambda kwargs: schema_helper.expand_column_types_if_needed(
                        **kwargs)
                }

                func_map[function](kwargs)
            else:
                try:
                    handle, status = schema_helper.execute_without_auto_commit(
                        part, handle)
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
    def open_connection(cfg, profile, connection):
        if connection.get('state') == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        result = connection.copy()

        try:
            handle = psycopg2.connect(PostgresAdapter.profile_to_spec(profile))

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
    def profile_to_spec(profile):
        return ("dbname='{}' user='{}' host='{}' password='{}' port='{}' "
                "connect_timeout=10".format(
                    profile.get('dbname'),
                    profile.get('user'),
                    profile.get('host'),
                    profile.get('password'),
                    profile.get('port'),
                ))
