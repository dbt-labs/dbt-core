from unittest import TestCase
from dbt.adapters.cache import RelationsCache
from dbt.adapters.base.relation import BaseRelation
from multiprocessing.dummy import Pool as ThreadPool
import dbt.exceptions

import random
import time
from dbt.flags import set_from_args
from argparse import Namespace

set_from_args(Namespace(WARN_ERROR=False), None)


def make_relation(database, schema, identifier):
    return BaseRelation.create(database=database, schema=schema, identifier=identifier)


def make_mock_relationship(database, schema, identifier):
    return BaseRelation.create(
        database=database, schema=schema, identifier=identifier, type="view"
    )


class TestCache(TestCase):
    def setUp(self):
        self.cache = RelationsCache()

    def assert_relations_state(self, database, schema, identifiers):
        relations = self.cache.get_relations(database, schema)
        for identifier, expect in identifiers.items():
            found = any(
                (r.identifier == identifier and r.schema == schema and r.database == database)
                for r in relations
            )
            msg = "{}.{}.{} was{} found in the cache!".format(
                database, schema, identifier, "" if found else " not"
            )
            self.assertEqual(expect, found, msg)

    def assert_relations_exist(self, database, schema, *identifiers):
        self.assert_relations_state(database, schema, {k: True for k in identifiers})

    def assert_relations_do_not_exist(self, database, schema, *identifiers):
        self.assert_relations_state(database, schema, {k: False for k in identifiers})


class TestEmpty(TestCache):
    def test_empty(self):
        self.assertEqual(len(self.cache.relations), 0)
        self.assertEqual(len(self.cache.get_relations("dbt", "test")), 0)


class TestDrop(TestCache):
    def setUp(self):
        super().setUp()
        self.cache.add(make_relation("dbt", "foo", "bar"))

    def test_missing_identifier_ignored(self):
        self.cache.drop(make_relation("dbt", "foo", "bar1"))
        self.assert_relations_exist("dbt", "foo", "bar")
        self.assertEqual(len(self.cache.relations), 1)

    def test_missing_schema_ignored(self):
        self.cache.drop(make_relation("dbt", "foo1", "bar"))
        self.assert_relations_exist("dbt", "foo", "bar")
        self.assertEqual(len(self.cache.relations), 1)

    def test_missing_db_ignored(self):
        self.cache.drop(make_relation("dbt1", "foo", "bar"))
        self.assert_relations_exist("dbt", "foo", "bar")
        self.assertEqual(len(self.cache.relations), 1)

    def test_drop(self):
        self.cache.drop(make_relation("dbt", "foo", "bar"))
        self.assert_relations_do_not_exist("dbt", "foo", "bar")
        self.assertEqual(len(self.cache.relations), 0)


class TestAddLink(TestCache):
    def setUp(self):
        super().setUp()
        self.cache.add(make_relation("dbt", "schema", "foo"))
        self.cache.add(make_relation("dbt_2", "schema", "bar"))
        self.cache.add(make_relation("dbt", "schema_2", "bar"))

    def test_no_src(self):
        self.assert_relations_exist("dbt", "schema", "foo")
        self.assert_relations_do_not_exist("dbt", "schema", "bar")

        self.cache.add_link(
            make_relation("dbt", "schema", "bar"), make_relation("dbt", "schema", "foo")
        )

        self.assert_relations_exist("dbt", "schema", "foo", "bar")

    def test_no_dst(self):
        self.assert_relations_exist("dbt", "schema", "foo")
        self.assert_relations_do_not_exist("dbt", "schema", "bar")

        self.cache.add_link(
            make_relation("dbt", "schema", "foo"), make_relation("dbt", "schema", "bar")
        )

        self.assert_relations_exist("dbt", "schema", "foo", "bar")


class TestRename(TestCache):
    def setUp(self):
        super().setUp()
        self.cache.add(make_relation("DBT", "schema", "foo"))
        self.assert_relations_exist("DBT", "schema", "foo")
        self.assertEqual(self.cache.schemas, {("dbt", "schema")})

    def test_no_source_error(self):
        # dest should be created anyway (it's probably a temp table)
        self.cache.rename(
            make_relation("DBT", "schema", "bar"), make_relation("DBT", "schema", "baz")
        )

        self.assertEqual(len(self.cache.relations), 2)
        self.assert_relations_exist("DBT", "schema", "foo", "baz")

    def test_dest_exists_error(self):
        foo = make_relation("DBT", "schema", "foo")
        bar = make_relation("DBT", "schema", "bar")
        self.cache.add(bar)
        self.assert_relations_exist("DBT", "schema", "foo", "bar")

        with self.assertRaises(dbt.exceptions.DbtInternalError):
            self.cache.rename(foo, bar)

        self.assert_relations_exist("DBT", "schema", "foo", "bar")

    def test_dest_different_db(self):
        self.cache.rename(
            make_relation("DBT", "schema", "foo"), make_relation("DBT_2", "schema", "foo")
        )
        self.assert_relations_exist("DBT_2", "schema", "foo")
        self.assert_relations_do_not_exist("DBT", "schema", "foo")
        # we know about both schemas: dbt has nothing, dbt_2 has something.
        self.assertEqual(self.cache.schemas, {("dbt_2", "schema"), ("dbt", "schema")})
        self.assertEqual(len(self.cache.relations), 1)

    def test_rename_identifier(self):
        self.cache.rename(
            make_relation("DBT", "schema", "foo"), make_relation("DBT", "schema", "bar")
        )

        self.assert_relations_exist("DBT", "schema", "bar")
        self.assert_relations_do_not_exist("DBT", "schema", "foo")
        self.assertEqual(self.cache.schemas, {("dbt", "schema")})

        relation = self.cache.relations[("dbt", "schema", "bar")]
        self.assertEqual(relation.inner.schema, "schema")
        self.assertEqual(relation.inner.identifier, "bar")
        self.assertEqual(relation.schema, "schema")
        self.assertEqual(relation.identifier, "bar")

    def test_rename_db(self):
        self.cache.rename(
            make_relation("DBT", "schema", "foo"), make_relation("DBT_2", "schema", "foo")
        )

        self.assertEqual(len(self.cache.get_relations("DBT", "schema")), 0)
        self.assertEqual(len(self.cache.get_relations("DBT_2", "schema")), 1)
        self.assert_relations_exist("DBT_2", "schema", "foo")
        self.assert_relations_do_not_exist("DBT", "schema", "foo")
        # we know about both schemas: dbt has nothing, dbt_2 has something.
        self.assertEqual(self.cache.schemas, {("dbt_2", "schema"), ("dbt", "schema")})

        relation = self.cache.relations[("dbt_2", "schema", "foo")]
        self.assertEqual(relation.inner.database, "DBT_2")
        self.assertEqual(relation.inner.schema, "schema")
        self.assertEqual(relation.inner.identifier, "foo")
        self.assertEqual(relation.database, "dbt_2")
        self.assertEqual(relation.schema, "schema")
        self.assertEqual(relation.identifier, "foo")

    def test_rename_schema(self):
        self.cache.rename(
            make_relation("DBT", "schema", "foo"), make_relation("DBT", "schema_2", "foo")
        )

        self.assertEqual(len(self.cache.get_relations("DBT", "schema")), 0)
        self.assertEqual(len(self.cache.get_relations("DBT", "schema_2")), 1)
        self.assert_relations_exist("DBT", "schema_2", "foo")
        self.assert_relations_do_not_exist("DBT", "schema", "foo")
        # we know about both schemas: schema has nothing, schema_2 has something.
        self.assertEqual(self.cache.schemas, {("dbt", "schema_2"), ("dbt", "schema")})

        relation = self.cache.relations[("dbt", "schema_2", "foo")]
        self.assertEqual(relation.inner.database, "DBT")
        self.assertEqual(relation.inner.schema, "schema_2")
        self.assertEqual(relation.inner.identifier, "foo")
        self.assertEqual(relation.database, "dbt")
        self.assertEqual(relation.schema, "schema_2")
        self.assertEqual(relation.identifier, "foo")


class TestGetRelations(TestCache):
    def setUp(self):
        super().setUp()
        self.relation = make_relation("dbt", "foo", "bar")
        self.cache.add(self.relation)

    def test_get_by_name(self):
        relations = self.cache.get_relations("dbt", "foo")
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], self.relation)

    def test_get_by_uppercase_schema(self):
        relations = self.cache.get_relations("dbt", "FOO")
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], self.relation)

    def test_get_by_uppercase_db(self):
        relations = self.cache.get_relations("DBT", "foo")
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], self.relation)

    def test_get_by_uppercase_schema_and_db(self):
        relations = self.cache.get_relations("DBT", "FOO")
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], self.relation)

    def test_get_by_wrong_db(self):
        relations = self.cache.get_relations("dbt_2", "foo")
        self.assertEqual(len(relations), 0)

    def test_get_by_wrong_schema(self):
        relations = self.cache.get_relations("dbt", "foo_2")
        self.assertEqual(len(relations), 0)


class TestAdd(TestCache):
    def setUp(self):
        super().setUp()
        self.relation = make_relation("dbt", "foo", "bar")
        self.cache.add(self.relation)

    def test_add(self):
        relations = self.cache.get_relations("dbt", "foo")
        self.assertEqual(len(relations), 1)
        self.assertEqual(len(self.cache.relations), 1)
        self.assertIs(relations[0], self.relation)

    def test_add_twice(self):
        # add a new relation with same name
        self.cache.add(make_relation("dbt", "foo", "bar"))
        self.assertEqual(len(self.cache.relations), 1)
        self.assertEqual(self.cache.schemas, {("dbt", "foo")})
        self.assert_relations_exist("dbt", "foo", "bar")

    def add_uppercase_schema(self):
        self.cache.add(make_relation("dbt", "FOO", "baz"))

        self.assertEqual(len(self.cache.relations), 2)
        relations = self.cache.get_relations("dbt", "foo")
        self.assertEqual(len(relations), 2)
        self.assertEqual(self.cache.schemas, {("dbt", "foo")})
        self.assertIsNot(self.cache.relations[("dbt", "foo", "bar")].inner, None)
        self.assertIsNot(self.cache.relations[("dbt", "foo", "baz")].inner, None)

    def add_different_db(self):
        self.cache.add(make_relation("dbt_2", "foo", "bar"))

        self.assertEqual(len(self.cache.relations), 2)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 1)
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 1)
        self.assertEqual(self.cache.schemas, {("dbt", "foo"), ("dbt_2", "foo")})
        self.assertIsNot(self.cache.relations[("dbt", "foo", "bar")].inner, None)
        self.assertIsNot(self.cache.relations[("dbt_2", "foo", "bar")].inner, None)


class TestLikeDbt(TestCase):
    def setUp(self):
        self.cache = RelationsCache()
        self._sleep = True

        # add a bunch of cache entries
        for ident in "abcdef":
            self.cache.add(make_relation("dbt", "schema", ident))
        # 'b' references 'a'
        self.cache.add_link(
            make_relation("dbt", "schema", "a"), make_relation("dbt", "schema", "b")
        )
        # and 'c' references 'b'
        self.cache.add_link(
            make_relation("dbt", "schema", "b"), make_relation("dbt", "schema", "c")
        )
        # and 'd' references 'b'
        self.cache.add_link(
            make_relation("dbt", "schema", "b"), make_relation("dbt", "schema", "d")
        )
        # and 'e' references 'a'
        self.cache.add_link(
            make_relation("dbt", "schema", "a"), make_relation("dbt", "schema", "e")
        )
        # and 'f' references 'd'
        self.cache.add_link(
            make_relation("dbt", "schema", "d"), make_relation("dbt", "schema", "f")
        )
        # so drop propagation goes (a -> (b -> (c (d -> f))) e)

    def assert_has_relations(self, expected):
        current = set(r.identifier for r in self.cache.get_relations("dbt", "schema"))
        self.assertEqual(current, expected)

    def test_drop_inner(self):
        self.assert_has_relations(set("abcdef"))
        self.cache.drop(make_relation("dbt", "schema", "b"))
        self.assert_has_relations({"a", "e"})

    def test_rename_and_drop(self):
        self.assert_has_relations(set("abcdef"))
        # drop the backup/tmp
        self.cache.drop(make_relation("dbt", "schema", "b__backup"))
        self.cache.drop(make_relation("dbt", "schema", "b__tmp"))
        self.assert_has_relations(set("abcdef"))
        # create a new b__tmp
        self.cache.add(
            make_relation(
                "dbt",
                "schema",
                "b__tmp",
            )
        )
        self.assert_has_relations(set("abcdef") | {"b__tmp"})
        # rename b -> b__backup
        self.cache.rename(
            make_relation("dbt", "schema", "b"), make_relation("dbt", "schema", "b__backup")
        )
        self.assert_has_relations(set("acdef") | {"b__tmp", "b__backup"})
        # rename temp to b
        self.cache.rename(
            make_relation("dbt", "schema", "b__tmp"), make_relation("dbt", "schema", "b")
        )
        self.assert_has_relations(set("abcdef") | {"b__backup"})

        # drop backup, everything that used to depend on b should be gone, but
        # b itself should still exist
        self.cache.drop(make_relation("dbt", "schema", "b__backup"))
        self.assert_has_relations(set("abe"))
        relation = self.cache.relations[("dbt", "schema", "a")]
        self.assertEqual(len(relation.referenced_by), 1)

    def _rand_sleep(self):
        if not self._sleep:
            return
        time.sleep(random.random() * 0.1)

    def _target(self, ident):
        self._rand_sleep()
        self.cache.rename(
            make_relation("dbt", "schema", ident),
            make_relation("dbt", "schema", ident + "__backup"),
        )
        self._rand_sleep()
        self.cache.add(make_relation("dbt", "schema", ident + "__tmp"))
        self._rand_sleep()
        self.cache.rename(
            make_relation("dbt", "schema", ident + "__tmp"), make_relation("dbt", "schema", ident)
        )
        self._rand_sleep()
        self.cache.drop(make_relation("dbt", "schema", ident + "__backup"))
        return ident, self.cache.get_relations("dbt", "schema")

    def test_threaded(self):
        # add three more short subchains for threads to test on
        for ident in "ghijklmno":
            make_mock_relationship("test_db", "schema", ident)
            self.cache.add(make_relation("dbt", "schema", ident))

        self.cache.add_link(
            make_relation("dbt", "schema", "a"), make_relation("dbt", "schema", "g")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "g"), make_relation("dbt", "schema", "h")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "h"), make_relation("dbt", "schema", "i")
        )

        self.cache.add_link(
            make_relation("dbt", "schema", "a"), make_relation("dbt", "schema", "j")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "j"), make_relation("dbt", "schema", "k")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "k"), make_relation("dbt", "schema", "l")
        )

        self.cache.add_link(
            make_relation("dbt", "schema", "a"), make_relation("dbt", "schema", "m")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "m"), make_relation("dbt", "schema", "n")
        )
        self.cache.add_link(
            make_relation("dbt", "schema", "n"), make_relation("dbt", "schema", "o")
        )

        pool = ThreadPool(4)
        results = list(pool.imap_unordered(self._target, ("b", "g", "j", "m")))
        pool.close()
        pool.join()
        # at a minimum, we expect each table to "see" itself, its parent ('a'),
        # and the unrelated table ('a')
        min_expect = {
            "b": {"a", "b", "e"},
            "g": {"a", "g", "e"},
            "j": {"a", "j", "e"},
            "m": {"a", "m", "e"},
        }

        for ident, relations in results:
            seen = set(r.identifier for r in relations)
            self.assertTrue(min_expect[ident].issubset(seen))

        self.assert_has_relations(set("abgjme"))

    def test_threaded_repeated(self):
        for _ in range(10):
            self.setUp()
            self._sleep = False
            self.test_threaded()


class TestComplexCache(TestCase):
    def setUp(self):
        self.cache = RelationsCache()
        inputs = [
            ("dbt", "foo", "table1"),
            ("dbt", "foo", "table3"),
            ("dbt", "foo", "table4"),
            ("dbt", "bar", "table2"),
            ("dbt", "bar", "table3"),
            ("dbt_2", "foo", "table1"),
            ("dbt_2", "foo", "table2"),
        ]
        self.inputs = [make_relation(d, s, i) for d, s, i in inputs]
        for relation in self.inputs:
            self.cache.add(relation)

        # dbt.foo.table3 references dbt.foo.table1
        # (create view dbt.foo.table3 as (select * from dbt.foo.table1...))
        self.cache.add_link(
            make_relation("dbt", "foo", "table1"), make_relation("dbt", "foo", "table3")
        )
        # dbt.bar.table3 references dbt.foo.table3
        # (create view dbt.bar.table5 as (select * from dbt.foo.table3...))
        self.cache.add_link(
            make_relation("dbt", "foo", "table3"), make_relation("dbt", "bar", "table3")
        )

        # dbt.foo.table4 also references dbt.foo.table1
        self.cache.add_link(
            make_relation("dbt", "foo", "table1"), make_relation("dbt", "foo", "table4")
        )

        # and dbt_2.foo.table1 references dbt.foo.table1
        self.cache.add_link(
            make_relation("dbt", "foo", "table1"),
            make_relation("dbt_2", "foo", "table1"),
        )

    def test_get_relations(self):
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 3)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 2)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 2)
        self.assertEqual(len(self.cache.relations), 7)

    def test_drop_one(self):
        # dropping dbt.bar.table2 should only drop itself
        self.cache.drop(make_relation("dbt", "bar", "table2"))
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 3)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 1)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 2)
        self.assertEqual(len(self.cache.relations), 6)

    def test_drop_many(self):
        # dropping dbt.foo.table1 should drop everything but dbt.bar.table2 and
        # dbt_2.foo.table2
        self.cache.drop(make_relation("dbt", "foo", "table1"))
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 0)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 1)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 1)
        self.assertEqual(len(self.cache.relations), 2)

    def test_rename_root(self):
        self.cache.rename(
            make_relation("dbt", "foo", "table1"), make_relation("dbt", "bar", "table1")
        )
        retrieved = self.cache.relations[("dbt", "bar", "table1")].inner
        self.assertEqual(retrieved.schema, "bar")
        self.assertEqual(retrieved.identifier, "table1")
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 2)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 3)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 2)
        self.assertEqual(len(self.cache.relations), 7)

        # make sure drops still cascade from the renamed table
        self.cache.drop(make_relation("dbt", "bar", "table1"))
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 0)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 1)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 1)
        self.assertEqual(len(self.cache.relations), 2)

    def test_rename_branch(self):
        self.cache.rename(
            make_relation("dbt", "foo", "table3"), make_relation("dbt", "foo", "table2")
        )
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 3)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 2)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 2)

        # make sure drops still cascade through the renamed table
        self.cache.drop(make_relation("dbt", "foo", "table1"))
        self.assertEqual(len(self.cache.get_relations("dbt", "foo")), 0)
        self.assertEqual(len(self.cache.get_relations("dbt", "bar")), 1)
        self.assertEqual(len(self.cache.get_relations("dbt_2", "foo")), 1)
        self.assertEqual(len(self.cache.relations), 2)
