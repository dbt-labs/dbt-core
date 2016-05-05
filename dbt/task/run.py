import pprint
import psycopg2
import os, sys
import fnmatch
import re

import sqlparse
import networkx as nx

class RedshiftTarget:
    def __init__(self, cfg):
        assert cfg['type'] == 'redshift'
        self.host = cfg['host']
        self.user = cfg['user']
        self.password = cfg['pass']
        self.port = cfg['port']
        self.dbname = cfg['dbname']
        self.schema = cfg['schema']

    def __get_spec(self):
        return "dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(
            self.dbname,
            self.user,
            self.host,
            self.password,
            self.port
        )

    def get_handle(self):
        return psycopg2.connect(self.__get_spec())


class Relation(object):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

    def valid(self):
        return None not in (self.schema, self.name)

    @property
    def val(self):
        return "{}.{}".format(self.schema, self.name)

    def __repr__(self):
        return self.val

    def __str__(self):
        return self.val

class Linker(object):
    def __init__(self, graph=None):
        if graph is None:
            self.graph = nx.DiGraph()
        else:
            self.graph = graph

        self.node_sql_map = {}

    def extract_name_and_deps(self, stmt):
        table_def = stmt.token_next_by_instance(0, sqlparse.sql.Identifier)
        schema, tbl_or_view =  table_def.get_parent_name(), table_def.get_real_name()
        if schema is None or tbl_or_view is None:
            raise RuntimeError('schema or view not defined?')

        definition = table_def.token_next_by_instance(0, sqlparse.sql.Parenthesis)

        definition_node = Relation(schema, tbl_or_view)

        local_defs = set()
        new_nodes = set()

        def extract_deps(stmt):
            token = stmt.token_first()
            while token is not None:
                excluded_types = [sqlparse.sql.Function] # don't dive into window functions
                if type(token) not in excluded_types and token.is_group():
                    # this is a thing that has a name -- note that!
                    local_defs.add(token.get_name())
                    # recurse into the group
                    extract_deps(token)

                if type(token) == sqlparse.sql.Identifier:
                    new_node = Relation(token.get_parent_name(), token.get_real_name())

                    if new_node.valid():
                        new_nodes.add(new_node) # don't add edges yet!

                index = stmt.token_index(token)
                token = stmt.token_next(index)

        extract_deps(definition)

        # only add nodes which don't reference locally defined constructs
        for new_node in new_nodes:
            if new_node.schema not in local_defs:
                self.graph.add_node(new_node.val)
                self.graph.add_edge(definition_node.val, new_node.val)

        return definition_node.val

    def as_dependency_list(self):
        order = nx.topological_sort(self.graph, reverse=True)
        for node in order:
            if node in self.node_sql_map: # TODO :
                yield (node, self.node_sql_map[node])
            else:
                pass

    def register(self, node, sql):
        if node in self.node_sql_map:
            raise RuntimeError("multiple declarations of node: {}".format(node))
        self.node_sql_map[node] = sql

    def link(self, sql):
        sql = sql.strip()
        for statement in sqlparse.parse(sql):
            if statement.get_type().startswith('CREATE'):
                node = self.extract_name_and_deps(statement)
                self.register(node, sql)
            else:
                print("Ignoring {}".format(sql[0:100].replace('\n', ' ')))

class RunTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

        self.linker = Linker()

    def __compiled_files(self):
        compiled_files = []
        sql_path = self.project['target-path']

        for root, dirs, files in os.walk(sql_path):
            for filename in files:
                if fnmatch.fnmatch(filename, "*.sql"):
                    abs_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(abs_path, sql_path)
                    compiled_files.append(rel_path)

        return compiled_files

    def __get_target(self):
        target_cfg = self.project.run_environment()
        if target_cfg['type'] == 'redshift':
            return RedshiftTarget(target_cfg)
        else:
            raise NotImplementedError("Unknown target type '{}'".format(target_cfg['type']))

    def __create_schema(self):
        target_cfg = self.project.run_environment()
        target = self.__get_target()
        with target.get_handle() as handle:
            with handle.cursor() as cursor:
                cursor.execute('create schema if not exists "{}"'.format(target_cfg['schema']))

    def __load_models(self):
        target = self.__get_target()
        for f in self.__compiled_files():
            with open(os.path.join(self.project['target-path'], f), 'r') as fh:
                self.linker.link(fh.read())

    def __query_for_existing(self, cursor, schema):
        sql = """
            select '{schema}.' || tablename as name, 'table' as type from pg_tables where schemaname = '{schema}'
                union all
            select '{schema}.' || viewname as name, 'view' as type from pg_views where schemaname = '{schema}' """.format(schema=schema)

        cursor.execute(sql)
        existing = [(name, relation_type) for (name, relation_type) in cursor.fetchall()]

        return dict(existing)

    def __drop(self, cursor, relation, relation_type):
        sql = "drop {relation_type} if exists {relation} cascade".format(relation_type=relation_type, relation=relation)
        cursor.execute(sql)

    def __execute_models(self):
        target = self.__get_target()

        with target.get_handle() as handle:
            with handle.cursor() as cursor:

                existing =  self.__query_for_existing(cursor, target.schema);
                dependency_list = list(self.linker.as_dependency_list())

                if len(dependency_list) == 0:
                    print("WARNING: Target directory is empty: '{}'. Try running `dbt compile`.".format(self.project['target-path']))
                    return

                for (relation, sql) in dependency_list:

                    if relation in existing:
                        self.__drop(cursor, relation, existing[relation])
                        handle.commit()

                    print("creating {}".format(relation))
                    #print("         {}...".format(re.sub( '\s+', ' ', sql[0:100] ).strip()))
                    cursor.execute(sql)
                    print("         {}".format(cursor.statusmessage))
                    handle.commit()

    def run(self):
        try:
            self.__create_schema()
            self.__load_models()
            self.__execute_models()
        except psycopg2.OperationalError as e:
            print("ERROR: Could not connect to the target database. Try `dbt debug` for more information")
            print(e.message)
            sys.exit(1)
