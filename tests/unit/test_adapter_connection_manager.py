import unittest
from unittest import mock
import sys

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

    def test_retry_connection(self):
        """Test a dummy handle is set on a connection on the first attempt.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects the Connection.handle attribute to be set to True and it's state to
        "open", after calling retry_connection.

        Moreover, the attribute should be set in the first attempt as no exception would
        be raised for retrying. A mock connect function is used to simulate a real connection
        passing on the first attempt.
        """
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            return True

        conn = BaseConnectionManager.retry_connection(
            conn,
            connect,
            self.logger,
            retryable_exceptions=[],
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert attempts == 1

    def test_retry_connection_fails_unhandled(self):
        """Test setting a handle fails upon raising a non-handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock connect function. As a
        result:
        * The Connection state should be "fail" and the handle None.
        * The resulting attempt count should be 1 as we are not explicitly configured to handle a
          ValueError.
        * retry_connection should raise a FailedToConnectError with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            raise ValueError("Something went horribly wrong")

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError, "Something went horribly wrong"
        ):

            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_limit=1,
                retry_timeout=lambda attempt: 0,
                retryable_exceptions=(TypeError,),
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 1

    def test_retry_connection_fails_handled(self):
        """Test setting a handle fails upon raising a handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock connect function.
        As a result:
        * The Connection state should be "fail" and the handle None.
        * The resulting attempt count should be 2 as we are configured to handle a ValueError.
        * retry_connection should raise a FailedToConnectError with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            raise ValueError("Something went horribly wrong")

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError, "Something went horribly wrong"
        ):

            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_timeout=0,
                retryable_exceptions=(ValueError,),
                retry_limit=1,
            )

        assert conn.state == "fail"
        assert conn.handle is None

    def test_retry_connection_passes_handled(self):
        """Test setting a handle fails upon raising a handled exception.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock connect function only the first
        time is called. Upon handling the exception once, connect should return.
        As a result:
        * The Connection state should be "open" and the handle True.
        * The resulting attempt count should be 2 as we are configured to handle a ValueError.
        """
        conn = self.postgres_connection
        is_handled = False
        attempts = 0

        def connect():
            nonlocal is_handled
            nonlocal attempts

            attempts += 1

            if is_handled:
                return True

            is_handled = True
            raise ValueError("Something went horribly wrong")

        conn = BaseConnectionManager.retry_connection(
            conn,
            connect,
            self.logger,
            retry_timeout=0,
            retryable_exceptions=(ValueError,),
            retry_limit=1,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_handled is True
        assert attempts == 2

    def test_retry_connection_attempts(self):
        """Test setting a handle fails upon raising a handled exception multiple times.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a ValueError to be raised by a mock connect function. As a result:
        * The Connection state should be "fail" and the handle None, as connect
          never returns.
        * The resulting attempt count should be 11 as we are configured to handle a ValueError.
        * retry_connection should raise a FailedToConnectError with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1

            raise ValueError("Something went horribly wrong")

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError, "Something went horribly wrong"
        ):
            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_timeout=0,
                retryable_exceptions=(ValueError,),
                retry_limit=10,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 11

    def test_retry_connection_fails_handling_all_exceptions(self):
        """Test setting a handle fails after exhausting all attempts.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a TypeError to be raised by a mock connect function. As a result:
        * The Connection state should be "fail" and the handle None, as connect
          never returns.
        * The resulting attempt count should be 11 as we are configured to handle all Exceptions.
        * retry_connection should raise a FailedToConnectError with the Exception message.
        """
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1

            raise TypeError("An unhandled thing went horribly wrong")

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError, "An unhandled thing went horribly wrong"
        ):
            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_timeout=0,
                retryable_exceptions=[Exception],
                retry_limit=15,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 16

    def test_retry_connection_passes_multiple_handled(self):
        """Test setting a handle passes upon handling multiple exceptions.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock connect to raise a ValueError in the first invocation and a
        TypeError in the second invocation. As a result:
        * The Connection state should be "open" and the handle True, as connect
          returns after both exceptions have been handled.
        * The resulting attempt count should be 3.
        """
        conn = self.postgres_connection
        is_value_err_handled = False
        is_type_err_handled = False
        attempts = 0

        def connect():
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

        conn = BaseConnectionManager.retry_connection(
            conn,
            connect,
            self.logger,
            retry_timeout=0,
            retryable_exceptions=(ValueError, TypeError),
            retry_limit=2,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_type_err_handled is True
        assert is_value_err_handled is True
        assert attempts == 3

    def test_retry_connection_passes_none_excluded(self):
        """Test setting a handle passes upon handling multiple exceptions.

        This test uses a Connection populated with test PostgresCredentials values, and
        expects a mock connect to raise a ValueError in the first invocation and a
        TypeError in the second invocation. As a result:
        * The Connection state should be "open" and the handle True, as connect
          returns after both exceptions have been handled.
        * The resulting attempt count should be 3.
        """
        conn = self.postgres_connection
        is_value_err_handled = False
        is_type_err_handled = False
        attempts = 0

        def connect():
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

        conn = BaseConnectionManager.retry_connection(
            conn,
            connect,
            self.logger,
            retry_timeout=0,
            retryable_exceptions=(ValueError, TypeError),
            retry_limit=2,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert is_type_err_handled is True
        assert is_value_err_handled is True
        assert attempts == 3

    def test_retry_connection_retry_limit(self):
        """Test retry_connection raises an exception with a negative retry limit."""
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            return True

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError, "retry_limit cannot be negative"
        ):
            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_timeout=0,
                retryable_exceptions=(ValueError,),
                retry_limit=-2,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 0

    def test_retry_connection_retry_timeout(self):
        """Test retry_connection raises an exception with a negative timeout."""
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            return True

        for retry_timeout in [-10, -2.5, lambda _: -100, lambda _: -10.1]:
            with self.assertRaisesRegex(
                dbt.exceptions.FailedToConnectError,
                "retry_timeout cannot be negative or return a negative time",
            ):
                BaseConnectionManager.retry_connection(
                    conn,
                    connect,
                    self.logger,
                    retry_timeout=-10,
                    retryable_exceptions=(ValueError,),
                    retry_limit=2,
                )

        assert conn.state == "init"
        assert conn.handle is None
        assert attempts == 0

    def test_retry_connection_exceeds_recursion_limit(self):
        """Test retry_connection raises an exception with retries that exceed recursion limit."""
        conn = self.postgres_connection
        attempts = 0

        def connect():
            nonlocal attempts
            attempts += 1
            return True

        with self.assertRaisesRegex(
            dbt.exceptions.FailedToConnectError,
            "retry_limit cannot be negative",
        ):
            BaseConnectionManager.retry_connection(
                conn,
                connect,
                self.logger,
                retry_timeout=2,
                retryable_exceptions=(ValueError,),
                retry_limit=sys.getrecursionlimit() + 1,
            )

        assert conn.state == "fail"
        assert conn.handle is None
        assert attempts == 0

    def test_retry_connection_with_exponential_backoff_timeout(self):
        """Test retry_connection with an exponential backoff timeout.

        We assert the provided exponential backoff function gets passed the right attempt number
        and produces the expected timeouts.
        """
        conn = self.postgres_connection
        attempts = 0
        timeouts = []

        def connect():
            nonlocal attempts
            attempts += 1

            if attempts < 12:
                raise ValueError("Keep trying!")
            return True

        def exp_backoff(n):
            nonlocal timeouts
            computed = 2**n
            # We store the computed values to ensure they match the expected backoff...
            timeouts.append((n, computed))
            # but we return 0 as we don't want the test to go on forever.
            return 0

        conn = BaseConnectionManager.retry_connection(
            conn,
            connect,
            self.logger,
            retry_timeout=exp_backoff,
            retryable_exceptions=(ValueError,),
            retry_limit=12,
        )

        assert conn.state == "open"
        assert conn.handle is True
        assert attempts == 12
        assert timeouts == [(n, 2**n) for n in range(12)]


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
        expects a mock connect to raise a psycopg2.errors.ConnectionFailuer
        in the first 3 invocations, after which the mock should return True. As a result:
        * The Connection state should be "open" and the handle True, as connect
          returns in the 4th attempt.
        * The resulting attempt count should be 4.
        """
        conn = self.connection
        attempt = 0

        def connect(*args, **kwargs):
            nonlocal attempt
            attempt += 1

            if attempt <= 2:
                raise psycopg2.errors.ConnectionFailure("Connection has failed")

            return True

        with mock.patch("psycopg2.connect", wraps=connect) as mock_connect:
            PostgresConnectionManager.open(conn)

            assert mock_connect.call_count == 3

        assert attempt == 3
        assert conn.state == "open"
        assert conn.handle is True
