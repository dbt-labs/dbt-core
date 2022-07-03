import unittest
from unittest import mock

import dbt.exceptions

import psycopg2

from dbt.contracts.connection import Connection
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.postgres import PostgresCredentials, PostgresConnectionManager
from dbt.events import AdapterLogger


class BaseConnectionManagerTest(unittest.TestCase):
    def setUp(self):
        self.postgres_credentials = PostgresCredentials(
            host="localhost",
            user="test-user",
            port=1111,
            password="test-password",
            database="test-db",
            schema="test-schema",
        )
        self.logger = AdapterLogger("test")
        self.postgres_connection = Connection("postgres", None, self.postgres_credentials)

    def test_set_connection_handle(self):
        conn = self.postgres_connection

        def acquire_handle():
            return True

        conn.credentials.acquire_handle = acquire_handle

        conn = BaseConnectionManager.set_connection_handle(conn, self.logger)

        assert conn.state == "open"
        assert conn.handle is True

    def test_set_connection_handle_fails_unhandled(self):
        conn = self.postgres_connection

        def acquire_handle():
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):

            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                timeout=0,
            )

        assert conn.state == "fail"
        assert conn.handle is None

    def test_set_connection_handle_fails_handled(self):
        conn = self.postgres_connection

        def acquire_handle():
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        handlers = {
            ValueError: None,
        }

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):

            BaseConnectionManager.set_connection_handle(
                conn, self.logger, timeout=0, exception_handlers=handlers
            )

        assert conn.state == "fail"
        assert conn.handle is None

    def test_set_connection_handle_passes_handled(self):
        conn = self.postgres_connection
        is_handled = False
        handled_exc = None

        def acquire_handle():
            nonlocal is_handled

            if is_handled:
                return True
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        def handle_value_error(e):
            nonlocal handled_exc
            nonlocal is_handled

            handled_exc = e
            is_handled = True

        handlers = {
            ValueError: handle_value_error,
        }

        conn = BaseConnectionManager.set_connection_handle(
            conn, self.logger, timeout=0, exception_handlers=handlers
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_handled is True
        assert isinstance(handled_exc, ValueError)
        assert handled_exc.args == ("Something went horribly wrong",)

    def test_set_connection_handle_attempts(self):
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1

            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        handlers = {
            ValueError: None,
        }

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):
            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                timeout=0,
                exception_handlers=handlers,
                retry_limit=10,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 11

    def test_set_connection_handle_attempts_with_retry_all(self):
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1

            raise TypeError("An unhandled thing went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        handlers = {
            ValueError: None,
        }

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "An unhandled thing went horribly wrong"
        ):
            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                timeout=0,
                exception_handlers=handlers,
                retry_all=True,
                retry_limit=15,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 16

    def test_set_connection_handle_passes_multiple_handled(self):
        conn = self.postgres_connection
        is_value_err_handled = False
        is_type_err_handled = False

        def acquire_handle():
            nonlocal is_value_err_handled
            nonlocal is_type_err_handled

            if is_value_err_handled and is_type_err_handled:
                return True
            elif is_type_err_handled:
                raise ValueError("Something went horribly wrong")
            else:
                raise TypeError("An unhandled thing went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        def handle_value_error(e):
            nonlocal is_value_err_handled
            is_value_err_handled = True

        def handle_type_error(e):
            nonlocal is_type_err_handled
            is_type_err_handled = True

        handlers = {
            ValueError: handle_value_error,
            TypeError: handle_type_error,
        }

        conn = BaseConnectionManager.set_connection_handle(
            conn,
            self.logger,
            timeout=0,
            exception_handlers=handlers,
            retry_limit=2,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_type_err_handled is True
        assert is_value_err_handled is True


class PostgresConnectionManagerTest(unittest.TestCase):
    def setUp(self):
        self.credentials = PostgresCredentials(
            host="localhost",
            user="test-user",
            port=1111,
            password="test-password",
            database="test-db",
            schema="test-schema",
            retry_timeout=0,
        )
        self.connection = Connection("postgres", None, self.credentials)

    def test_open(self):
        conn = self.connection
        attempt = 0

        def acquire_handle(*args, **kwargs):
            nonlocal attempt
            attempt += 1

            if attempt <= 3:
                raise psycopg2.errors.ConnectionFailure("Connection has failed")

            return True

        conn.credentials.acquire_handle = acquire_handle

        PostgresConnectionManager.open(conn)

        assert attempt == 4
        assert conn.state == "open"
        assert conn.handle is True
