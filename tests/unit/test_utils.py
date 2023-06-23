import unittest

import dbt.exceptions
import dbt.utils


class TestDeepMerge(unittest.TestCase):
    def test__simple_cases(self):
        cases = [
            {"args": [{}, {"a": 1}], "expected": {"a": 1}, "description": "one key into empty"},
            {
                "args": [{}, {"b": 1}, {"a": 1}],
                "expected": {"a": 1, "b": 1},
                "description": "three merges",
            },
        ]

        for case in cases:
            actual = dbt.utils.deep_merge(*case["args"])
            self.assertEqual(
                case["expected"],
                actual,
                "failed on {} (actual {}, expected {})".format(
                    case["description"], actual, case["expected"]
                ),
            )


class TestMerge(unittest.TestCase):
    def test__simple_cases(self):
        cases = [
            {"args": [{}, {"a": 1}], "expected": {"a": 1}, "description": "one key into empty"},
            {
                "args": [{}, {"b": 1}, {"a": 1}],
                "expected": {"a": 1, "b": 1},
                "description": "three merges",
            },
        ]

        for case in cases:
            actual = dbt.utils.deep_merge(*case["args"])
            self.assertEqual(
                case["expected"],
                actual,
                "failed on {} (actual {}, expected {})".format(
                    case["description"], actual, case["expected"]
                ),
            )


class TestDeepMap(unittest.TestCase):
    def setUp(self):
        self.input_value = {
            "foo": {
                "bar": "hello",
                "baz": [1, 90.5, "990", "89.9"],
            },
            "nested": [
                {
                    "test": "90",
                    "other_test": None,
                },
                {
                    "test": 400,
                    "other_test": 4.7e9,
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
            "foo": {
                "bar": -1,
                "baz": [1, 90, 990, -1],
            },
            "nested": [
                {
                    "test": 90,
                    "other_test": -1,
                },
                {
                    "test": 400,
                    "other_test": 4700000000,
                },
            ],
        }
        actual = dbt.utils.deep_map_render(self.intify_all, self.input_value)
        self.assertEqual(actual, expected)

        actual = dbt.utils.deep_map_render(self.intify_all, expected)
        self.assertEqual(actual, expected)

    @staticmethod
    def special_keypath(value, keypath):

        if tuple(keypath) == ("foo", "baz", 1):
            return "hello"
        else:
            return value

    def test__keypath(self):
        expected = {
            "foo": {
                "bar": "hello",
                # the only change from input is the second entry here
                "baz": [1, "hello", "990", "89.9"],
            },
            "nested": [
                {
                    "test": "90",
                    "other_test": None,
                },
                {
                    "test": 400,
                    "other_test": 4.7e9,
                },
            ],
        }
        actual = dbt.utils.deep_map_render(self.special_keypath, self.input_value)
        self.assertEqual(actual, expected)

        actual = dbt.utils.deep_map_render(self.special_keypath, expected)
        self.assertEqual(actual, expected)

    def test__noop(self):
        actual = dbt.utils.deep_map_render(lambda x, _: x, self.input_value)
        self.assertEqual(actual, self.input_value)

    def test_trivial(self):
        cases = [[], {}, 1, "abc", None, True]
        for case in cases:
            result = dbt.utils.deep_map_render(lambda x, _: x, case)
            self.assertEqual(result, case)

        with self.assertRaises(dbt.exceptions.DbtConfigError):
            dbt.utils.deep_map_render(lambda x, _: x, {"foo": object()})


class TestMultiDict(unittest.TestCase):
    def test_one_member(self):
        dct = {"a": 1, "b": 2, "c": 3}
        md = dbt.utils.MultiDict([dct])
        assert len(md) == 3
        for key in "abc":
            assert key in md
        assert md["a"] == 1
        assert md["b"] == 2
        assert md["c"] == 3

    def test_two_members_no_overlap(self):
        first = {"a": 1, "b": 2, "c": 3}
        second = {"d": 1, "e": 2, "f": 3}
        md = dbt.utils.MultiDict([first, second])
        assert len(md) == 6
        for key in "abcdef":
            assert key in md
        assert md["a"] == 1
        assert md["b"] == 2
        assert md["c"] == 3
        assert md["d"] == 1
        assert md["e"] == 2
        assert md["f"] == 3

    def test_two_members_overlap(self):
        first = {"a": 1, "b": 2, "c": 3}
        second = {"c": 1, "d": 2, "e": 3}
        md = dbt.utils.MultiDict([first, second])
        assert len(md) == 5
        for key in "abcde":
            assert key in md
        assert md["a"] == 1
        assert md["b"] == 2
        assert md["c"] == 1
        assert md["d"] == 2
        assert md["e"] == 3


class TestHumanizeExecutionTime(unittest.TestCase):
    def test_humanzing_execution_time_with_integer(self):

        result = dbt.utils.humanize_execution_time(execution_time=9460)

        assert result == " in 2 hours 37 minutes and 40.00 seconds"

    def test_humanzing_execution_time_with_two_decimal_place_float(self):

        result = dbt.utils.humanize_execution_time(execution_time=0.32)

        assert result == " in 0 hours 0 minutes and 0.32 seconds"

    def test_humanzing_execution_time_with_four_decimal_place_float(self):

        result = dbt.utils.humanize_execution_time(execution_time=0.3254)

        assert result == " in 0 hours 0 minutes and 0.33 seconds"
