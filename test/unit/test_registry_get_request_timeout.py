import unittest
from requests.exceptions import Timeout

from dbt.clients.registry import _get

class testRegistryGetRequestTimeout(unittest.TestCase):
    def test_registry_request_timeout(self):
        # using non routable IP to test time out logic in the _get function
        self.assertRaises(Timeout, _get, '', 'http://10.255.255.1')
