import unittest
from requests.exceptions import Timeout

from dbt.clients.registry import _get

class testRegistryGetRequestTimeout(unittest.TestCase):
    def test_registry_request_timeout(self):
        # using closed port to test timeout logic in the _get function
        self.assertRaises(Timeout, _get, '', 'https://www.getdbt.com:81')
