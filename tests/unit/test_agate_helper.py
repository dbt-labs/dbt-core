import unittest

import agate

from datetime import datetime
from decimal import Decimal
from isodate import tzinfo
import os
from shutil import rmtree
from tempfile import mkdtemp
from dbt.clients import agate_helper

SAMPLE_CSV_DATA = """a,b,c,d,e,f,g
1,n,test,3.2,20180806T11:33:29.320Z,True,NULL
2,y,asdf,900,20180806T11:35:29.320Z,False,a string"""

SAMPLE_CSV_BOM_DATA = "\ufeff" + SAMPLE_CSV_DATA


EXPECTED = [
    [
        1,
        "n",
        "test",
        Decimal("3.2"),
        datetime(2018, 8, 6, 11, 33, 29, 320000, tzinfo=tzinfo.Utc()),
        True,
        None,
    ],
    [
        2,
        "y",
        "asdf",
        900,
        datetime(2018, 8, 6, 11, 35, 29, 320000, tzinfo=tzinfo.Utc()),
        False,
        "a string",
    ],
]


EXPECTED_STRINGS = [
    ["1", "n", "test", "3.2", "20180806T11:33:29.320Z", "True", None],
    ["2", "y", "asdf", "900", "20180806T11:35:29.320Z", "False", "a string"],
]


class TestAgateHelper(unittest.TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir)

    def test_from_csv(self):
        path = os.path.join(self.tempdir, "input.csv")
        with open(path, "wb") as fp:
            fp.write(SAMPLE_CSV_DATA.encode("utf-8"))
        tbl = agate_helper.from_csv(path, ())
        self.assertEqual(len(tbl), len(EXPECTED))
        for idx, row in enumerate(tbl):
            self.assertEqual(list(row), EXPECTED[idx])

    def test_bom_from_csv(self):
        path = os.path.join(self.tempdir, "input.csv")
        with open(path, "wb") as fp:
            fp.write(SAMPLE_CSV_BOM_DATA.encode("utf-8"))
        tbl = agate_helper.from_csv(path, ())
        self.assertEqual(len(tbl), len(EXPECTED))
        for idx, row in enumerate(tbl):
            self.assertEqual(list(row), EXPECTED[idx])

    def test_from_csv_all_reserved(self):
        path = os.path.join(self.tempdir, "input.csv")
        with open(path, "wb") as fp:
            fp.write(SAMPLE_CSV_DATA.encode("utf-8"))
        tbl = agate_helper.from_csv(path, tuple("abcdefg"))
        self.assertEqual(len(tbl), len(EXPECTED_STRINGS))
        for expected, row in zip(EXPECTED_STRINGS, tbl):
            self.assertEqual(list(row), expected)

    def test_from_data(self):
        column_names = ["a", "b", "c", "d", "e", "f", "g"]
        data = [
            {
                "a": "1",
                "b": "n",
                "c": "test",
                "d": "3.2",
                "e": "20180806T11:33:29.320Z",
                "f": "True",
                "g": "NULL",
            },
            {
                "a": "2",
                "b": "y",
                "c": "asdf",
                "d": "900",
                "e": "20180806T11:35:29.320Z",
                "f": "False",
                "g": "a string",
            },
        ]
        tbl = agate_helper.table_from_data(data, column_names)
        self.assertEqual(len(tbl), len(EXPECTED))
        for idx, row in enumerate(tbl):
            self.assertEqual(list(row), EXPECTED[idx])

    def test_datetime_formats(self):
        path = os.path.join(self.tempdir, "input.csv")
        datetimes = [
            "20180806T11:33:29.000Z",
            "20180806T11:33:29Z",
            "20180806T113329Z",
        ]
        expected = datetime(2018, 8, 6, 11, 33, 29, 0, tzinfo=tzinfo.Utc())
        for dt in datetimes:
            with open(path, "wb") as fp:
                fp.write("a\n{}".format(dt).encode("utf-8"))
            tbl = agate_helper.from_csv(path, ())
            self.assertEqual(tbl[0][0], expected)

    def test_merge_allnull(self):
        t1 = agate_helper.table_from_rows([(1, "a", None), (2, "b", None)], ("a", "b", "c"))
        t2 = agate_helper.table_from_rows([(3, "c", None), (4, "d", None)], ("a", "b", "c"))
        result = agate_helper.merge_tables([t1, t2])
        self.assertEqual(result.column_names, ("a", "b", "c"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate_helper.Integer)
        self.assertEqual(len(result), 4)

    def test_merge_mixed(self):
        t1 = agate_helper.table_from_rows(
            [(1, "a", None, None), (2, "b", None, None)], ("a", "b", "c", "d")
        )
        t2 = agate_helper.table_from_rows(
            [(3, "c", "dog", 1), (4, "d", "cat", 5)], ("a", "b", "c", "d")
        )
        t3 = agate_helper.table_from_rows(
            [(3, "c", None, 1.5), (4, "d", None, 3.5)], ("a", "b", "c", "d")
        )

        result = agate_helper.merge_tables([t1, t2])
        self.assertEqual(result.column_names, ("a", "b", "c", "d"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate.data_types.Text)
        assert isinstance(result.column_types[3], agate_helper.Integer)
        self.assertEqual(len(result), 4)

        result = agate_helper.merge_tables([t1, t3])
        self.assertEqual(result.column_names, ("a", "b", "c", "d"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate_helper.Integer)
        assert isinstance(result.column_types[3], agate.data_types.Number)
        self.assertEqual(len(result), 4)

        result = agate_helper.merge_tables([t2, t3])
        self.assertEqual(result.column_names, ("a", "b", "c", "d"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate.data_types.Text)
        assert isinstance(result.column_types[3], agate.data_types.Number)
        self.assertEqual(len(result), 4)

        result = agate_helper.merge_tables([t3, t2])
        self.assertEqual(result.column_names, ("a", "b", "c", "d"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate.data_types.Text)
        assert isinstance(result.column_types[3], agate.data_types.Number)
        self.assertEqual(len(result), 4)

        result = agate_helper.merge_tables([t1, t2, t3])
        self.assertEqual(result.column_names, ("a", "b", "c", "d"))
        assert isinstance(result.column_types[0], agate_helper.Integer)
        assert isinstance(result.column_types[1], agate.data_types.Text)
        assert isinstance(result.column_types[2], agate.data_types.Text)
        assert isinstance(result.column_types[3], agate.data_types.Number)
        self.assertEqual(len(result), 6)

    def test_nocast_string_types(self):
        # String fields should not be coerced into a representative type
        # See: https://github.com/dbt-labs/dbt-core/issues/2984

        column_names = ["a", "b", "c", "d", "e"]
        result_set = [
            {"a": "0005", "b": "01T00000aabbccdd", "c": "true", "d": 10, "e": False},
            {"a": "0006", "b": "01T00000aabbccde", "c": "false", "d": 11, "e": True},
        ]

        tbl = agate_helper.table_from_data_flat(data=result_set, column_names=column_names)
        self.assertEqual(len(tbl), len(result_set))

        expected = [
            ["0005", "01T00000aabbccdd", "true", Decimal(10), False],
            ["0006", "01T00000aabbccde", "false", Decimal(11), True],
        ]

        for i, row in enumerate(tbl):
            self.assertEqual(list(row), expected[i])

    def test_nocast_bool_01(self):
        # True and False values should not be cast to 1 and 0, and vice versa
        # See: https://github.com/dbt-labs/dbt-core/issues/4511

        column_names = ["a", "b"]
        result_set = [
            {"a": True, "b": 1},
            {"a": False, "b": 0},
        ]

        tbl = agate_helper.table_from_data_flat(data=result_set, column_names=column_names)
        self.assertEqual(len(tbl), len(result_set))

        assert isinstance(tbl.column_types[0], agate.data_types.Boolean)
        assert isinstance(tbl.column_types[1], agate_helper.Integer)

        expected = [
            [True, Decimal(1)],
            [False, Decimal(0)],
        ]

        for i, row in enumerate(tbl):
            self.assertEqual(list(row), expected[i])
