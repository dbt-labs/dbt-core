import unittest

from decimal import Decimal

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


class TestMaxDigits(unittest.TestCase):

    def test__simple_cases(self):
        self.assertEquals(max_digits([Decimal('0.003')]), 4)
        self.assertEquals(max_digits([Decimal('1.003')]), 4)
        self.assertEquals(max_digits([Decimal('1003')]), 4)
        self.assertEquals(max_digits([Decimal('10003')]), 5)
        self.assertEquals(max_digits([Decimal('0.00003')]), 6)
