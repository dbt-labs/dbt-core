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
        """Test a dummy handle is set on a connection on the first attempt.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects the Connection.handle attribute to be set to True and it's state to
        "open". Moreover, this must happen in the first attempt as no exception would
        be raised for retrying. A mock acquire_handle is set to simulate a real connection
        passing on the first attempt.
        """
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1
            return True

        conn.credentials.acquire_handle = acquire_handle

        conn = BaseConnectionManager.set_connection_handle(conn, self.logger)

        assert conn.state == "open"
        assert conn.handle is True
        assert attempts == 1

    def test_set_connection_handle_fails_unhandled(self):
        """Test setting a handle fails upon raising a non-handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock acquire_handle function. As a
        result:
        * The Connection state should be "fail" and the handle None.
        * The resulting attempt count should be 1 as we are not explicitly configured to handle a
          ValueError.
        * set_connection_handle should raise a FailedToConnectException with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):

            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                retry_limit=1,
                timeout=lambda attempt, max_attempts: 0,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 1

    def test_set_connection_handle_fails_handled(self):
        """Test setting a handle fails upon raising a handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock acquire_handle function.
        As a result:
        * The Connection state should be "fail" and the handle None.
        * The resulting attempt count should be 2 as we are configured to handle a ValueError.
        * set_connection_handle should raise a FailedToConnectException with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):

            BaseConnectionManager.set_connection_handle(
                conn, self.logger, timeout=0, retry_on_exceptions=(ValueError,), retry_limit=1,
            )

        assert conn.state == "fail"
        assert conn.handle is None

    def test_set_connection_handle_passes_handled(self):
        """Test setting a handle fails upon raising a handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock acquire_handle function only the first
        time is called. Upon handling the exception once, acquire_handle should return.
        As a result:
        * The Connection state should be "open" and the handle True.
        * The resulting attempt count should be 2 as we are configured to handle a ValueError.
        """
        conn = self.postgres_connection
        is_handled = False
        attempts = 0

        def acquire_handle():
            nonlocal is_handled
            nonlocal attempts

            attempts += 1

            if is_handled:
                return True

            is_handled = True
            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        conn = BaseConnectionManager.set_connection_handle(
            conn, self.logger, timeout=0, retry_on_exceptions=(ValueError,), retry_limit=1
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_handled is True
        assert attempts == 2

    def test_set_connection_handle_attempts(self):
        """Test setting a handle fails upon raising a handled exception multiple times.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock acquire_handle function. As a result:
        * The Connection state should be "fail" and the handle None, as acquire_handle
          never returns.
        * The resulting attempt count should be 11 as we are configured to handle a ValueError.
        * set_connection_handle should raise a FailedToConnectException with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1

            raise ValueError("Something went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "Something went horribly wrong"
        ):
            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                timeout=0,
                retry_on_exceptions=(ValueError,),
                retry_limit=10,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 11

    def test_set_connection_handle_fails_handling_all_exceptions(self):
        """Test setting a handle fails after exhausting all attempts.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a TypeError to be raised by a mock acquire_handle function. As a result:
        * The Connection state should be "fail" and the handle None, as acquire_handle
          never returns.
        * The resulting attempt count should be 11 as we are configured to handle everything
          via an empty list to not_retry_on_exception.
        * set_connection_handle should raise a FailedToConnectException with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def acquire_handle():
            nonlocal attempts
            attempts += 1

            raise TypeError("An unhandled thing went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectException, "An unhandled thing went horribly wrong"
        ):
            BaseConnectionManager.set_connection_handle(
                conn,
                self.logger,
                timeout=0,
                not_retry_on_exceptions=[],
                retry_limit=15,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 16

    def test_set_connection_handle_passes_multiple_handled(self):
        """Test setting a handle passes upon handling multiple exceptions.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock acquire_handle to raise a ValueError in the first invocation and a
        TypeError in the second invocation. As a result:
        * The Connection state should be "open" and the handle True, as acquire_handle
          returns after both exceptions have been handled.
        * The resulting attempt count should be 3.
        """
        conn = self.postgres_connection
        is_value_err_handled = False
        is_type_err_handled = False
        attempts = 0

        def acquire_handle():
            nonlocal is_value_err_handled
            nonlocal is_type_err_handled
            nonlocal attempts

            attempts += 1

            if is_value_err_handled and is_type_err_handled:
                return True
            elif is_type_err_handled:
                is_value_err_handled = True
                raise ValueError("Something went horribly wrong")
            else:
                is_type_err_handled = True
                raise TypeError("An unhandled thing went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        conn = BaseConnectionManager.set_connection_handle(
            conn,
            self.logger,
            timeout=0,
            retry_on_exceptions=(ValueError, TypeError),
            retry_limit=2,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_type_err_handled is True
        assert is_value_err_handled is True
        assert attempts == 3

    def test_set_connection_handle_passes_none_excluded(self):
        """Test setting a handle passes upon handling multiple exceptions.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock acquire_handle to raise a ValueError in the first invocation and a
        TypeError in the second invocation. As a result:
        * The Connection state should be "open" and the handle True, as acquire_handle
          returns after both exceptions have been handled.
        * The resulting attempt count should be 3.
        """
        conn = self.postgres_connection
        is_value_err_handled = False
        is_type_err_handled = False
        attempts = 0

        def acquire_handle():
            nonlocal is_value_err_handled
            nonlocal is_type_err_handled
            nonlocal attempts

            attempts += 1

            if is_value_err_handled and is_type_err_handled:
                return True
            elif is_type_err_handled:
                is_value_err_handled = True
                raise ValueError("Something went horribly wrong")
            else:
                is_type_err_handled = True
                raise TypeError("An unhandled thing went horribly wrong")

        conn.credentials.acquire_handle = acquire_handle

        conn = BaseConnectionManager.set_connection_handle(
            conn,
            self.logger,
            timeout=0,
            retry_on_exceptions=(ValueError, TypeError),
            retry_limit=2,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_type_err_handled is True
        assert is_value_err_handled is True
        assert attempts == 3


class PostgresConnectionManagerTest(unittest.TestCase):
    def setUp(self):
        self.credentials = PostgresCredentials(
            host="localhost",
            user="test-user",
            port=1111,
            password="test-password",
            database="test-db",
            schema="test-schema",
            retries=2,
        )
        self.connection = Connection("postgres", None, self.credentials)

    def test_open(self):
        """Test opening a Postgres Connection with failures in the first 3 attempts.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock acquire_handle to raise a psycopg2.errors.ConnectionFailuer
        in the first 3 invocations, after which the mock should return True. As a result:
        * The Connection state should be "open" and the handle True, as acquire_handle
          returns in the 4th attempt.
        * The resulting attempt count should be 4.
        """
        conn = self.connection
        attempt = 0

        def acquire_handle(*args, **kwargs):
            nonlocal attempt
            attempt += 1

            if attempt <= 2:
                raise psycopg2.errors.ConnectionFailure("Connection has failed")

            return True

        conn.credentials.acquire_handle = acquire_handle

        PostgresConnectionManager.open(conn)

        assert attempt == 3
        assert conn.state == "open"
        assert conn.handle is True
