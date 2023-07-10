import unittest

from dbt.exceptions import ConnectionError
from dbt.clients.registry import _get_with_retries


class testRegistryGetRequestException(unittest.TestCase):
    def test_registry_request_error_catching(self):
        # using non routable IP to test connection error logic in the _get_with_retries function
        self.assertRaises(ConnectionError, _get_with_retries, "", "http://0.0.0.0")
