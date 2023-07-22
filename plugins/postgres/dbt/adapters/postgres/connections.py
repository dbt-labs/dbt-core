from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import string_types

import boto3

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.dataclass_schema import StrEnum
from hologram.helpers import StrLiteral
from dbt.events import AdapterLogger

from dbt.helper_types import Port
from dataclasses import dataclass
from typing import Any, Optional


logger = AdapterLogger("Postgres")


class PostgresConnectionMethod(StrEnum):
    DATABASE = "database"
    IAM = "iam"


@dataclass
class PostgresCredentials(Credentials):
    host: str
    user: str
    port: Port
    password: Optional[str] = None
    connect_timeout: int = 10
    method: Optional[PostgresConnectionMethod] = PostgresConnectionMethod.DATABASE
    iam_profile: Optional[str] = None
    region: Optional[str] = None
    role: Optional[str] = None
    search_path: Optional[str] = None
    keepalives_idle: int = 0  # 0 means to use the default value
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    application_name: Optional[str] = "dbt"
    retries: int = 1

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "postgres"

    @property
    def unique_field(self):
        return self.host

    @classmethod
    def validate(cls, data: Any):
        super(Credentials, cls).validate(data)

        method_credentials = {
            PostgresConnectionMethod.DATABASE: PostgresCredentialsDatabase,
            PostgresConnectionMethod.IAM: PostgresCredentialsIAM,
        }

        method_credentials[data.get("method", PostgresConnectionMethod.DATABASE)].validate(data)

    def _connection_keys(self):
        return (
            "host",
            "port",
            "user",
            "database",
            "schema",
            "connect_timeout",
            "method",
            "iam_profile",
            "region",
            "role",
            "search_path",
            "keepalives_idle",
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "application_name",
            "retries",
        )


@dataclass
class PostgresCredentialsDatabase(PostgresCredentials):
    password: str
    method: Optional[
        StrLiteral(PostgresConnectionMethod.DATABASE)
    ] = PostgresConnectionMethod.DATABASE

    @classmethod
    def validate(cls, data: Any):
        super(Credentials, cls).validate(data)


@dataclass
class PostgresCredentialsIAM(PostgresCredentials):
    password: None
    method: StrLiteral(PostgresConnectionMethod.IAM)
    iam_profile: Optional[str] = None
    region: Optional[str] = None

    @classmethod
    def validate(cls, data: Any):
        super(Credentials, cls).validate(data)


class PostgresConnectMethodFactory:
    credentials: PostgresCredentials

    def __init__(self, credentials):
        self.credentials = credentials

    def get_connect_method(self):
        method = self.credentials.method
        kwargs = {
            "host": self.credentials.host,
            "dbname": self.credentials.database,
            "port": int(self.credentials.port) if self.credentials.port else int(5432),
            "user": self.credentials.user,
            "connect_timeout": self.credentials.connect_timeout,
        }

        # we don't want to pass 0 along to connect() as postgres will try to
        # call an invalid setsockopt() call (contrary to the docs).
        if self.credentials.keepalives_idle:
            kwargs["keepalives_idle"] = self.credentials.keepalives_idle

        # psycopg2 doesn't support search_path officially,
        # see https://github.com/psycopg/psycopg2/issues/465
        search_path = self.credentials.search_path
        if search_path is not None and search_path != "":
            # see https://postgresql.org/docs/9.5/libpq-connect.html
            kwargs["options"] = "-c search_path={}".format(search_path.replace(" ", "\\ "))

        if self.credentials.sslmode:
            kwargs["sslmode"] = self.credentials.sslmode

        if self.credentials.sslcert is not None:
            kwargs["sslcert"] = self.credentials.sslcert

        if self.credentials.sslkey is not None:
            kwargs["sslkey"] = self.credentials.sslkey

        if self.credentials.sslrootcert is not None:
            kwargs["sslrootcert"] = self.credentials.sslrootcert

        if self.credentials.application_name:
            kwargs["application_name"] = self.credentials.application_name

        # Support missing 'method' for backwards compatibility
        if method == PostgresConnectionMethod.DATABASE or method is None:

            def connect():
                logger.debug("Connecting to postgres with username/password based auth...")
                c = psycopg2.connect(
                    password=self.credentials.password,
                    **kwargs,
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

        elif method == PostgresConnectionMethod.IAM:

            def connect():
                logger.debug("Connecting to postgres with IAM based auth...")

                session_kwargs = {}
                if self.credentials.iam_profile:
                    session_kwargs["profile_name"] = self.credentials.iam_profile
                if self.credentials.region:
                    session_kwargs["region_name"] = self.credentials.region
                session = boto3.Session(**session_kwargs)

                client = session.client("rds")
                generate_db_auth_token_kwargs = {
                    "DBHostname": self.credentials.host,
                    "Port": self.credentials.port,
                    "DBUsername": self.credentials.user,
                }
                if self.credentials.region:
                    generate_db_auth_token_kwargs["Region"] = self.credentials.region
                token = client.generate_db_auth_token(**generate_db_auth_token_kwargs)

                kwargs["password"] = token

                c = psycopg2.connect(
                    **kwargs,
                )
                if self.credentials.role:
                    c.cursor().execute("set role {}".format(self.credentials.role))
                return c

        else:
            raise dbt.exceptions.FailedToConnectError(
                "Invalid 'method' in profile: '{}'".format(method)
            )

        return connect


class PostgresConnectionManager(SQLConnectionManager):
    TYPE = "postgres"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except psycopg2.DatabaseError as e:
            logger.debug("Postgres error: {}".format(str(e)))

            try:
                self.rollback_if_open()
            except psycopg2.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        connect_method_factory = PostgresConnectMethodFactory(credentials)

        retryable_exceptions = [
            # OperationalError is subclassed by all psycopg2 Connection Exceptions and it's raised
            # by generic connection timeouts without an error code. This is a limitation of
            # psycopg2 which doesn't provide subclasses for errors without a SQLSTATE error code.
            # The limitation has been known for a while and there are no efforts to tackle it.
            # See: https://github.com/psycopg/psycopg2/issues/682
            psycopg2.errors.OperationalError,
        ]

        def exponential_backoff(attempt: int):
            return attempt * attempt

        return cls.retry_connection(
            connection,
            connect=connect_method_factory.get_connect_method(),
            logger=logger,
            retry_limit=credentials.retries,
            retry_timeout=exponential_backoff,
            retryable_exceptions=retryable_exceptions,
        )

    def cancel(self, connection):
        connection_name = connection.name
        try:
            pid = connection.handle.get_backend_pid()
        except psycopg2.InterfaceError as exc:
            # if the connection is already closed, not much to cancel!
            if "already closed" in str(exc):
                logger.debug(f"Connection {connection_name} was already closed")
                return
            # probably bad, re-raise it
            raise

        sql = "select pg_terminate_backend({})".format(pid)

        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))

        _, cursor = self.add_query(sql)
        res = cursor.fetchone()

        logger.debug("Cancel query '{}': {}".format(connection_name, res))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = str(cursor.statusmessage)
        rows = cursor.rowcount
        status_message_parts = message.split() if message is not None else []
        status_messsage_strings = [part for part in status_message_parts if not part.isdigit()]
        code = " ".join(status_messsage_strings)
        return AdapterResponse(_message=message, code=code, rows_affected=rows)

    @classmethod
    def data_type_code_to_name(cls, type_code: int) -> str:
        return string_types[type_code].name
