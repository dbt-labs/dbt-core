import unittest

import dbt.utils


class TestDeepMerge(unittest.TestCase):

    def test__simple_cases(self):
        cases = [
            {'args': [{}, {'a': 1}],
             'expected': {'a': 1},
             'description': 'one key into empty'},
            {'args': [{}, {'b': 1}, {'a': 1}],
             'expected': {'a': 1, 'b': 1},
             'description': 'three merges'},
        ]

        for case in cases:
            actual = dbt.utils.deep_merge(*case['args'])
            self.assertEquals(
                case['expected'], actual,
                'failed on {} (actual {}, expected {})'.format(
                    case['description'], actual, case['expected']))


class TestMerge(unittest.TestCase):

    def test__simple_cases(self):
        cases = [
            {'args': [{}, {'a': 1}],
             'expected': {'a': 1},
             'description': 'one key into empty'},
            {'args': [{}, {'b': 1}, {'a': 1}],
             'expected': {'a': 1, 'b': 1},
             'description': 'three merges'},
        ]

        for case in cases:
            actual = dbt.utils.deep_merge(*case['args'])
            self.assertEquals(
                case['expected'], actual,
                'failed on {} (actual {}, expected {})'.format(
                    case['description'], actual, case['expected']))
