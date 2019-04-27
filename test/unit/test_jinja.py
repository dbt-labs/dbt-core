import unittest

from dbt.clients.jinja import get_template

class TestJinja(unittest.TestCase):
    def test_do(self):
        s = '{% set my_dict = {} %}\n{% do my_dict.update(a=1) %}'

        template = get_template(s, {})
        mod = template.make_module()
        self.assertEqual(mod.my_dict, {'a': 1})
