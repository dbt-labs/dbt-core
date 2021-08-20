import requests
import unittest

from dbt.exceptions import ConnectionException
from dbt.utils import _connection_exception_retry as connection_exception_retry


class TestCoreDbtUtils(unittest.TestCase):
    def test_connection_exception_retry_none(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add(), 5)
        self.assertEqual(1, counter)

    def test_connection_exception_retry_max(self):
        Counter._reset()
        with self.assertRaises(ConnectionException):
            connection_exception_retry(lambda: Counter._add_with_exception(), 5)
        self.assertEqual(6, counter) # 6 = original attempt plus 5 retries

    def test_connection_exception_retry_success(self):
        Counter._reset()
        connection_exception_retry(lambda: Counter._add_with_limited_exception(), 5)
        self.assertEqual(2, counter) # 2 = original attempt plus 1 retry


counter:int = 0 
class Counter():
    def _add():
        global counter
        counter+=1
    def _add_with_exception():
        global counter
        counter+=1
        raise requests.exceptions.ConnectionError
    def _add_with_limited_exception():
        global counter
        counter+=1
        if counter < 2:
            raise requests.exceptions.ConnectionError
    def _reset():
        global counter
        counter = 0
