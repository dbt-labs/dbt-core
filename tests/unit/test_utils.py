import unittest

import dbt.exceptions
import dbt.utils


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
