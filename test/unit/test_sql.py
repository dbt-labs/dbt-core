import unittest

import dbt.sql


class TestSqlManipulations(unittest.TestCase):

    def assertIdentity(self, sql):
        self.assertEqual(dbt.sql.hoist_ctes(sql), sql)

    def test__hoist_ctes__identities(self):
        self.assertIdentity("'with a as (with b as (select * from s.b) select * from b) select * from a'")  # noqa
        self.assertIdentity("grant select on *.* to user fishtown")  # noqa
        self.assertIdentity("select * from table")  # noqa
        self.assertIdentity("select * from with order by 1 desc")  # noqa
        self.assertIdentity("drop table a cascade")  # noqa
        self.assertIdentity("with a as (with b as (select * from s.b) select * from b select * from a'")  # noqa

    def test__hoist_ctes(self):
        start = 'with a as (with b as (select * from s.b) select * from b) select * from a'  # noqa
        expected = 'with b as (select * from s.b), a as (select * from b) select * from a'  # noqa

        self.assertEqual(dbt.sql.hoist_ctes(start), expected)

    def test__hoist_ctes__from_create_view(self):
        start = 'create view d as with a as (with b as (select * from s.b) select * from b) select * from a'  # noqa
        expected = 'with b as (select * from s.b), a as (select * from b) create view d as select * from a'  # noqa

        self.assertEqual(dbt.sql.hoist_ctes(start), expected)

    def test__hoist_ctes__from_create_table(self):
        start = 'with intermediate as (with b as (select * from s.b) select * from b) select * into b from intermediate'  # noqa
        expected = 'with b as (select * from s.b), intermediate as (select * from b) select * into b from intermediate'  # noqa

        self.assertEqual(dbt.sql.hoist_ctes(start), expected)
