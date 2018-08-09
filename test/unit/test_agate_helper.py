from __future__ import unicode_literals
import unittest

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


EXPECTED = [
    [1, 'n', 'test', Decimal('3.2'),
     datetime(2018, 8, 6, 11, 33, 29, 320000, tzinfo=tzinfo.Utc()),
     True, None,
    ],
    [2, 'y', 'asdf', 900,
     datetime(2018, 8, 6, 11, 35, 29, 320000, tzinfo=tzinfo.Utc()),
     False, 'a string',
    ],
]

class TestAgateHelper(unittest.TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir)

    def test_from_csv(self):
        path = os.path.join(self.tempdir, 'input.csv')
        with open(path, 'wb') as fp:
            fp.write(SAMPLE_CSV_DATA.encode('utf-8'))
        tbl = agate_helper.from_csv(path)
        self.assertEqual(len(tbl), len(EXPECTED))
        for idx, row in enumerate(tbl):
            self.assertEqual(list(row), EXPECTED[idx])

    def test_from_data(self):
        column_names = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        data = [
            {'a': '1', 'b': 'n', 'c': 'test', 'd': '3.2',
             'e': '20180806T11:33:29.320Z', 'f': 'True', 'g': 'NULL'},
            {'a': '2', 'b': 'y', 'c': 'asdf', 'd': '900',
             'e': '20180806T11:35:29.320Z', 'f': 'False', 'g': 'a string'}
        ]
        tbl = agate_helper.table_from_data(data, column_names)
        self.assertEqual(len(tbl), len(EXPECTED))
        for idx, row in enumerate(tbl):
            self.assertEqual(list(row), EXPECTED[idx])
