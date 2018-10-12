from unittest import TestCase
from dbt.adapters.cache import RelationsCache
from dbt.adapters.default.relation import DefaultRelation
from multiprocessing.dummy import Pool as ThreadPool
import dbt.exceptions

import random
import time


def make_relation(schema, identifier):
    return DefaultRelation.create(schema=schema, identifier=identifier)

def make_mock_relationship(schema, identifier):
    return DefaultRelation.create(
        database='test_db', schema=schema, identifier=identifier,
        table_name=identifier, type='view'
    )


class TestCache(TestCase):
    def setUp(self):
        self.cache = RelationsCache()

    def test_empty(self):
        self.assertEqual(len(self.cache.relations), 0)
        relations = self.cache.get_relations('test')
        self.assertEqual(len(relations), 0)

    def test_bad_drop(self):
        self.cache.drop(make_relation('foo', 'bar'))

    def test_bad_link(self):
        self.cache.add(make_relation('schema', 'foo'))
        # src does not exist
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.add_link(make_relation('schema', 'bar'),
                                make_relation('schema', 'foo'))

        # dst does not exist
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.add_link(make_relation('schema', 'foo'),
                                make_relation('schema', 'bar'))

    def test_bad_rename(self):
        # foo does not exist - should be ignored
        self.cache.rename(make_relation('schema', 'foo'),
                          make_relation('schema', 'bar'))

        self.cache.add(make_relation('schema', 'foo'))
        self.cache.add(make_relation('schema', 'bar'))
        # bar exists
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.rename(make_relation('schema', 'foo'),
                              make_relation('schema', 'bar'))

    def test_get_relations(self):
        relation = make_relation('foo', 'bar')
        self.cache.add(relation)
        self.assertEqual(len(self.cache.relations), 1)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], relation)

        relations = self.cache.get_relations('FOO')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], relation)

    def test_add(self):
        rel = make_relation('foo', 'bar')
        self.cache.add(rel)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], rel)

        # add a new relation with same name
        self.cache.add(make_relation('foo', 'bar'))
        self.assertEqual(len(self.cache.relations), 1)
        self.assertEqual(self.cache.schemas, {'foo'})

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], rel)

        self.cache.add(make_relation('FOO', 'baz'))
        self.assertEqual(len(self.cache.relations), 2)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 2)

        self.assertEqual(self.cache.schemas, {'foo', 'FOO'})
        self.assertIsNot(self.cache.relations[('foo', 'bar')].inner, None)
        self.assertIsNot(self.cache.relations[('FOO', 'baz')].inner, None)

    def test_rename(self):
        self.cache.add(make_relation('foo', 'bar'))
        self.assertIsNot(self.cache.relations[('foo', 'bar')].inner, None)
        self.cache.rename(make_relation('foo', 'bar'),
                          make_relation('foo', 'baz'))

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].schema, 'foo')
        self.assertEqual(relations[0].identifier, 'baz')

        relation = self.cache.relations[('foo', 'baz')]
        self.assertEqual(relation.inner.schema, 'foo')
        self.assertEqual(relation.inner.identifier, 'baz')
        self.assertEqual(relation.schema, 'foo')
        self.assertEqual(relation.identifier, 'baz')

        with self.assertRaises(KeyError):
            self.cache.relations[('foo', 'bar')]


class TestLikeDbt(TestCase):
    def setUp(self):
        self.cache = RelationsCache()
        self._sleep = True

        # add a bunch of cache entries
        for ident in 'abcdef':
            self.cache.add(make_relation('schema', ident))
        # 'b' references 'a'
        self.cache.add_link(make_relation('schema', 'a'),
                            make_relation('schema', 'b'))
        # and 'c' references 'b'
        self.cache.add_link(make_relation('schema', 'b'),
                            make_relation('schema', 'c'))
        # and 'd' references 'b'
        self.cache.add_link(make_relation('schema', 'b'),
                            make_relation('schema', 'd'))
        # and 'e' references 'a'
        self.cache.add_link(make_relation('schema', 'a'),
                            make_relation('schema', 'e'))
        # and 'f' references 'd'
        self.cache.add_link(make_relation('schema', 'd'),
                            make_relation('schema', 'f'))
        # so drop propagation goes (a -> (b -> (c (d -> f))) e)

    def assert_has_relations(self, expected):
        current = set(r.identifier for r in self.cache.get_relations('schema'))
        self.assertEqual(current, expected)

    def test_drop_inner(self):
        self.assert_has_relations(set('abcdef'))
        self.cache.drop(make_relation('schema', 'b'))
        self.assert_has_relations({'a', 'e'})

    def test_rename_and_drop(self):
        self.assert_has_relations(set('abcdef'))
        # drop the backup/tmp
        self.cache.drop(make_relation('schema', 'b__backup'))
        self.cache.drop(make_relation('schema', 'b__tmp'))
        self.assert_has_relations(set('abcdef'))
        # create a new b__tmp
        self.cache.add(make_relation('schema', 'b__tmp',))
        self.assert_has_relations(set('abcdef') | {'b__tmp'})
        # rename b -> b__backup
        self.cache.rename(make_relation('schema', 'b'),
                          make_relation('schema', 'b__backup'))
        self.assert_has_relations(set('acdef') | {'b__tmp', 'b__backup'})
        # rename temp to b
        self.cache.rename(make_relation('schema', 'b__tmp'),
                          make_relation('schema', 'b'))
        self.assert_has_relations(set('abcdef') | {'b__backup'})

        # drop backup, everything that used to depend on b should be gone, but
        # b itself should still exist
        self.cache.drop(make_relation('schema', 'b__backup'))
        self.assert_has_relations(set('abe'))
        relation = self.cache.relations[('schema', 'a')]
        self.assertEqual(len(relation.referenced_by), 1)

    def _rand_sleep(self):
        if not self._sleep:
            return
        time.sleep(random.random() * 0.1)

    def _target(self, ident):
        self._rand_sleep()
        self.cache.rename(make_relation('schema', ident),
                          make_relation('schema', ident+'__backup'))
        self._rand_sleep()
        self.cache.add(make_relation('schema', ident+'__tmp')
        )
        self._rand_sleep()
        self.cache.rename(make_relation('schema', ident+'__tmp'),
                          make_relation('schema', ident))
        self._rand_sleep()
        self.cache.drop(make_relation('schema', ident+'__backup'))
        return ident, self.cache.get_relations('schema')

    def test_threaded(self):
        # add three more short subchains for threads to test on
        for ident in 'ghijklmno':
            obj = make_mock_relationship('schema', ident)
            self.cache.add(make_relation('schema', ident))

        self.cache.add_link(make_relation('schema', 'a'),
                            make_relation('schema', 'g'))
        self.cache.add_link(make_relation('schema', 'g'),
                            make_relation('schema', 'h'))
        self.cache.add_link(make_relation('schema', 'h'),
                            make_relation('schema', 'i'))

        self.cache.add_link(make_relation('schema', 'a'),
                            make_relation('schema', 'j'))
        self.cache.add_link(make_relation('schema', 'j'),
                            make_relation('schema', 'k'))
        self.cache.add_link(make_relation('schema', 'k'),
                            make_relation('schema', 'l'))

        self.cache.add_link(make_relation('schema', 'a'),
                            make_relation('schema', 'm'))
        self.cache.add_link(make_relation('schema', 'm'),
                            make_relation('schema', 'n'))
        self.cache.add_link(make_relation('schema', 'n'),
                            make_relation('schema', 'o'))

        pool = ThreadPool(4)
        results = list(pool.imap_unordered(self._target, ('b', 'g', 'j', 'm')))
        pool.close()
        pool.join()
        # at a minimum, we expect each table to "see" itself, its parent ('a'),
        # and the unrelated table ('a')
        min_expect = {
            'b': {'a', 'b', 'e'},
            'g': {'a', 'g', 'e'},
            'j': {'a', 'j', 'e'},
            'm': {'a', 'm', 'e'},
        }

        for ident, relations in results:
            seen = set(r.identifier for r in relations)
            self.assertTrue(min_expect[ident].issubset(seen))

        self.assert_has_relations(set('abgjme'))

    def test_threaded_repeated(self):
        for _ in range(10):
            self.setUp()
            self._sleep = False
            self.test_threaded()



class TestComplexCache(TestCase):
    def setUp(self):
        self.cache = RelationsCache()
        inputs = [
            ('foo', 'table1'),
            ('bar', 'table2'),
            ('foo', 'table3'),
            ('foo', 'table4'),
            ('bar', 'table3'),
        ]
        self.inputs = [make_relation(s, i) for s, i in inputs]
        for relation in self.inputs:
            self.cache.add(relation)

        # foo.table3 references foo.table1
        # (create view table3 as (select * from table1...))
        self.cache.add_link(
            make_relation('foo', 'table1'),
            make_relation('foo', 'table3')
        )
        # bar.table3 references foo.table3
        # (create view bar.table5 as (select * from foo.table3...))
        self.cache.add_link(
            make_relation('foo', 'table3'),
            make_relation('bar', 'table3')
        )

        # foo.table2 also references foo.table1
        self.cache.add_link(
            make_relation('foo', 'table1'),
            make_relation('foo', 'table4')
        )

    def test_get_relations(self):
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 2)
        self.assertEqual(len(self.cache.relations), 5)

    def test_drop_one(self):
        # dropping bar.table2 should only drop itself
        self.cache.drop(make_relation('bar', 'table2'))
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 4)

    def test_drop_many(self):
        # dropping foo.table1 should drop everything but bar.table2.
        self.cache.drop(make_relation('foo', 'table1'))
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)

    def test_rename_root(self):
        self.cache.rename(make_relation('foo', 'table1'),
                          make_relation('bar', 'table1'))
        retrieved = self.cache.relations[('bar', 'table1')].inner
        self.assertEqual(retrieved.schema, 'bar')
        self.assertEqual(retrieved.identifier, 'table1')
        self.assertEqual(len(self.cache.get_relations('foo')), 2)
        self.assertEqual(len(self.cache.get_relations('bar')), 3)

        # make sure drops still cascade from the renamed table
        self.cache.drop(make_relation('bar', 'table1'))
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)

    def test_rename_branch(self):
        self.cache.rename(make_relation('foo', 'table3'),
                          make_relation('foo', 'table2'))
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 2)

        # make sure drops still cascade through the renamed table
        self.cache.drop(make_relation('foo', 'table1'))
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)
