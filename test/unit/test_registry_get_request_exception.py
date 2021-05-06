import unittest

from dbt.exceptions import RegistryException
from dbt.clients.registry import _get

class testRegistryGetRequestException(unittest.TestCase):
    def test_registry_request_error_catching(self):
        # using non routable IP to test connection error logic in the _get function
        self.assertRaises(RegistryException, _get, '', 'http://10.255.255.1')
