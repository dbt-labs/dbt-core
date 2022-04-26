import functools
import pytest
from requests.exceptions import RequestException
from dbt.exceptions import ConnectionException
from dbt.utils import _connection_exception_retry


def no_retry_fn():
    return "success"


class TestNoRetries:
    def test_no_retry(self):
        fn_to_retry = functools.partial(no_retry_fn)
        result = _connection_exception_retry(fn_to_retry, 3)

        expected = "success"

        assert result == expected


def no_success_fn():
    raise RequestException("You'll never pass")
    return "failure"


class TestMaxRetries:
    def test_no_retry(self):
        fn_to_retry = functools.partial(no_success_fn)

        with pytest.raises(ConnectionException):
            _connection_exception_retry(fn_to_retry, 3)


def single_retry_fn():
    global counter
    if counter == 0:
        counter += 1
        raise RequestException("You won't pass this one time")
    elif counter == 1:
        counter += 1
        return "success on 2"

    return "How did we get here?"


class TestSingleRetry:
    def test_no_retry(self):
        global counter
        counter = 0

        fn_to_retry = functools.partial(single_retry_fn)
        result = _connection_exception_retry(fn_to_retry, 3)
        expected = "success on 2"

        # We need to test the return value here, not just that it did not throw an error.
        # If the value is not being passed it causes cryptic errors
        assert result == expected
        assert counter == 2
