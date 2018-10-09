import unittest

import dbt.exceptions
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


class TestDeepMap(unittest.TestCase):
    def setUp(self):
        self.input_value = {
            'foo': {
                'bar': 'hello',
                'baz': [1, 90.5, '990', '89.9'],
            },
            'nested': [
                {
                    'test': '90',
                    'other_test': None,
                },
                {
                    'test': 400,
                    'other_test': 4.7e9,
                },
            ],
        }

    @staticmethod
    def intify_all(value, _):
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

    def test__simple_cases(self):
        expected = {
            'foo': {
                'bar': -1,
                'baz': [1, 90, 990, -1],
            },
            'nested': [
                {
                    'test': 90,
                    'other_test': -1,
                },
                {
                    'test': 400,
                    'other_test': 4700000000,
                },
            ],
        }
        actual = dbt.utils.deep_map(self.intify_all, self.input_value)
        self.assertEquals(actual, expected)

        actual = dbt.utils.deep_map(self.intify_all, expected)
        self.assertEquals(actual, expected)


    @staticmethod
    def special_keypath(value, keypath):

        if tuple(keypath) == ('foo', 'baz', 1):
            return 'hello'
        else:
            return value

    def test__keypath(self):
        expected = {
            'foo': {
                'bar': 'hello',
                # the only change from input is the second entry here
                'baz': [1, 'hello', '990', '89.9'],
            },
            'nested': [
                {
                    'test': '90',
                    'other_test': None,
                },
                {
                    'test': 400,
                    'other_test': 4.7e9,
                },
            ],
        }
        actual = dbt.utils.deep_map(self.special_keypath, self.input_value)
        self.assertEquals(actual, expected)

        actual = dbt.utils.deep_map(self.special_keypath, expected)
        self.assertEquals(actual, expected)

    def test__noop(self):
        actual = dbt.utils.deep_map(lambda x, _: x, self.input_value)
        self.assertEquals(actual, self.input_value)

    def test_trivial(self):
        cases = [[], {}, 1, 'abc', None, True]
        for case in cases:
            result = dbt.utils.deep_map(lambda x, _: x, case)
            self.assertEquals(result, case)

        with self.assertRaises(dbt.exceptions.DbtConfigError):
            dbt.utils.deep_map(lambda x, _: x, {'foo': object()})

