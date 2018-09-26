from unittest import TestCase
from dbt.adapters.cache import RelationsCache
from dbt.adapters.default.relation import DefaultRelation
from multiprocessing.dummy import Pool as ThreadPool
import dbt.exceptions

import random
import time


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
        self.cache.drop('foo', 'bar')

    def test_bad_link(self):
        self.cache.add('schema', 'foo', object())
        # src does not exist
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.add_link('schema', 'bar', 'schema', 'foo')

        # dst does not exist
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.add_link('schema', 'foo', 'schema', 'bar')

    def test_bad_rename(self):
        # foo does not exist - should be ignored
        self.cache.rename('schema', 'foo', 'schema', 'bar')

        self.cache.add('schema', 'foo', object())
        self.cache.add('schema', 'bar', object())
        # bar exists
        with self.assertRaises(dbt.exceptions.InternalException):
            self.cache.rename('schema', 'foo', 'schema', 'bar')

    def test_get_relations(self):
        obj = object()
        self.cache.add('foo', 'bar', inner=obj)
        self.assertEqual(len(self.cache.relations), 1)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], obj)

        relations = self.cache.get_relations('FOO')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], obj)

    def test_add(self):
        obj = object()
        self.cache.add('foo', 'bar', inner=obj)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], obj)

        self.cache.add('foo', 'bar', inner=obj)
        self.assertEqual(len(self.cache.relations), 1)
        self.assertEqual(self.cache.schemas, {'foo'})

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertIs(relations[0], obj)

        self.cache.add('FOO', 'baz', inner=object())
        self.assertEqual(len(self.cache.relations), 2)

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 2)

        self.assertEqual(self.cache.schemas, {'foo', 'FOO'})
        self.assertIsNot(self.cache._get_cache_value('foo', 'bar').inner, None)
        self.assertIsNot(self.cache._get_cache_value('FOO', 'baz').inner, None)

    def test_rename(self):
        obj = make_mock_relationship('foo', 'bar')
        self.cache.add('foo', 'bar', inner=obj)
        self.assertIsNot(self.cache._get_cache_value('foo', 'bar').inner, None)
        self.cache.rename('foo', 'bar', 'foo', 'baz')

        relations = self.cache.get_relations('foo')
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].schema, 'foo')
        self.assertEqual(relations[0].identifier, 'baz')

        relation = self.cache._get_cache_value('foo', 'baz')
        self.assertEqual(relation.inner.schema, 'foo')
        self.assertEqual(relation.inner.identifier, 'baz')
        self.assertEqual(relation.schema, 'foo')
        self.assertEqual(relation.identifier, 'baz')

        with self.assertRaises(KeyError):
            self.cache._get_cache_value('foo', 'bar')


class TestLikeDbt(TestCase):
    def setUp(self):
        self.cache = RelationsCache()
        self._sleep = True

        # add a bunch of cache entries
        for ident in 'abcdef':
            obj = make_mock_relationship('schema', ident)
            self.cache.add('schema', ident, inner=obj)
        # 'b' references 'a'
        self.cache.add_link('schema', 'a', 'schema', 'b')
        # and 'c' references 'b'
        self.cache.add_link('schema', 'b', 'schema', 'c')
        # and 'd' references 'b'
        self.cache.add_link('schema', 'b', 'schema', 'd')
        # and 'e' references 'a'
        self.cache.add_link('schema', 'a', 'schema', 'e')
        # and 'f' references 'd'
        self.cache.add_link('schema', 'd', 'schema', 'f')
        # so drop propagation goes (a -> (b -> (c (d -> f))) e)

    def assert_has_relations(self, expected):
        current = set(r.identifier for r in self.cache.get_relations('schema'))
        self.assertEqual(current, expected)

    def test_drop_inner(self):
        self.assert_has_relations(set('abcdef'))
        self.cache.drop('schema', 'b')
        self.assert_has_relations({'a', 'e'})

    def test_rename_and_drop(self):
        self.assert_has_relations(set('abcdef'))
        # drop the backup/tmp
        self.cache.drop('schema', 'b__backup')
        self.cache.drop('schema', 'b__tmp')
        self.assert_has_relations(set('abcdef'))
        # create a new b__tmp
        self.cache.add(
            'schema', 'b__tmp',
            make_mock_relationship('schema', 'b__tmp')
        )
        self.assert_has_relations(set('abcdef') | {'b__tmp'})
        # rename b -> b__backup
        self.cache.rename('schema', 'b', 'schema', 'b__backup')
        self.assert_has_relations(set('acdef') | {'b__tmp', 'b__backup'})
        # rename temp to b
        self.cache.rename('schema', 'b__tmp', 'schema', 'b')
        self.assert_has_relations(set('abcdef') | {'b__backup'})

        # drop backup, everything that used to depend on b should be gone, but
        # b itself should still exist
        self.cache.drop('schema', 'b__backup')
        self.assert_has_relations(set('abe'))
        relation = self.cache._get_cache_value('schema', 'a')
        self.assertEqual(len(relation.referenced_by), 1)

    def _rand_sleep(self):
        if not self._sleep:
            return
        time.sleep(random.random() * 0.1)

    def _target(self, ident):
        self._rand_sleep()
        self.cache.rename('schema', ident, 'schema', ident+'__backup')
        self._rand_sleep()
        self.cache.add(
            'schema', ident+'__tmp',
            make_mock_relationship('schema', 'b__tmp')
        )
        self._rand_sleep()
        self.cache.rename('schema', ident+'__tmp', 'schema', ident)
        self._rand_sleep()
        self.cache.drop('schema', ident+'__backup')
        return ident, self.cache.get_relations('schema')

    def test_threaded(self):
        # add three more short subchains for threads to test on
        for ident in 'ghijklmno':
            obj = make_mock_relationship('schema', ident)
            self.cache.add('schema', ident, inner=obj)

        self.cache.add_link('schema', 'a', 'schema', 'g')
        self.cache.add_link('schema', 'g', 'schema', 'h')
        self.cache.add_link('schema', 'h', 'schema', 'i')

        self.cache.add_link('schema', 'a', 'schema', 'j')
        self.cache.add_link('schema', 'j', 'schema', 'k')
        self.cache.add_link('schema', 'k', 'schema', 'l')

        self.cache.add_link('schema', 'a', 'schema', 'm')
        self.cache.add_link('schema', 'm', 'schema', 'n')
        self.cache.add_link('schema', 'n', 'schema', 'o')

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
        self.inputs = [(s, i, make_mock_relationship(s, i)) for s, i in inputs]
        for schema, ident, inner in self.inputs:
            self.cache.add(schema, ident, inner)

        # foo.table3 references foo.table1
        # (create view table3 as (select * from table1...))
        self.cache.add_link(
            'foo', 'table1',
            'foo', 'table3'
        )
        # bar.table3 references foo.table3
        # (create view bar.table5 as (select * from foo.table3...))
        self.cache.add_link(
            'foo', 'table3',
            'bar', 'table3'
        )

        # foo.table2 also references foo.table1
        self.cache.add_link(
            'foo', 'table1',
            'foo', 'table4',
        )

    def test_get_relations(self):
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 2)
        self.assertEqual(len(self.cache.relations), 5)

    def test_drop_one(self):
        # dropping bar.table2 should only drop itself
        self.cache.drop('bar', 'table2')
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 4)

    def test_drop_many(self):
        # dropping foo.table1 should drop everything but bar.table2.
        self.cache.drop('foo', 'table1')
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)

    def test_rename_root(self):
        self.cache.rename('foo', 'table1', 'bar', 'table1')
        retrieved = self.cache._get_cache_value('bar','table1').inner
        self.assertEqual(retrieved.schema, 'bar')
        self.assertEqual(retrieved.identifier, 'table1')
        self.assertEqual(len(self.cache.get_relations('foo')), 2)
        self.assertEqual(len(self.cache.get_relations('bar')), 3)

        # make sure drops still cascade from the renamed table
        self.cache.drop('bar', 'table1')
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)

    def test_rename_branch(self):
        self.cache.rename('foo', 'table3', 'foo', 'table2')
        self.assertEqual(len(self.cache.get_relations('foo')), 3)
        self.assertEqual(len(self.cache.get_relations('bar')), 2)

        # make sure drops still cascade through the renamed table
        self.cache.drop('foo', 'table1')
        self.assertEqual(len(self.cache.get_relations('foo')), 0)
        self.assertEqual(len(self.cache.get_relations('bar')), 1)
        self.assertEqual(len(self.cache.relations), 1)

