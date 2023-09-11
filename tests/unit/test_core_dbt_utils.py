import requests
import tarfile
import unittest

from dbt.exceptions import ConnectionError
from dbt.utils import _connection_exception_retry as connection_exception_retry


class TestCoreDbtUtils(unittest.TestCase):
    def test_connection_exception_retry_none(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add(), 5)
        self.assertEqual(1, counter)

    def test_connection_exception_retry_success_requests_exception(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add_with_requests_exception(), 5)
        self.assertEqual(2, counter)  # 2 = original attempt returned None, plus 1 retry

    def test_connection_exception_retry_max(self):
        Counter._reset()
        with self.assertRaises(ConnectionError):
            connection_exception_retry(lambda: Counter._add_with_exception(), 5)
        self.assertEqual(6, counter)  # 6 = original attempt plus 5 retries

    def test_connection_exception_retry_success_failed_untar(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add_with_untar_exception(), 5)
        self.assertEqual(2, counter)  # 2 = original attempt returned ReadError, plus 1 retry

    def test_connection_exception_retry_success_failed_eofexception(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add_with_eof_exception(), 5)
        self.assertEqual(2, counter)  # 2 = original attempt returned EOFError, plus 1 retry


counter: int = 0


class Counter:
    def _add():
        global counter
        counter += 1

    # All exceptions that Requests explicitly raises inherit from
    # requests.exceptions.RequestException so we want to make sure that raises plus one exception
    # that inherit from it for sanity
    def _add_with_requests_exception():
        global counter
        counter += 1
        if counter < 2:
            raise requests.exceptions.RequestException

    def _add_with_exception():
        global counter
        counter += 1
        raise requests.exceptions.ConnectionError

    def _add_with_untar_exception():
        global counter
        counter += 1
        if counter < 2:
            raise tarfile.ReadError

    def _add_with_eof_exception():
        global counter
        counter += 1
        if counter < 2:
            raise EOFError

    def _reset():
        global counter
        counter = 0
