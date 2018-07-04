import itertools
import os
import json
from collections import OrderedDict, defaultdict
import sqlparse

import dbt.project
import dbt.utils
import dbt.include
import dbt.tracking

from dbt.utils import get_materialization, NodeType, is_type

from dbt.linker import Linker

import dbt.compat
import dbt.context.runtime
import dbt.contracts.graph.compiled
import dbt.contracts.project
import dbt.exceptions
import dbt.flags
import dbt.loader

from dbt.clients.system import write_file
from dbt.logger import GLOBAL_LOGGER as logger

graph_file_name = 'graph.gpickle'
manifest_file_name = 'manifest.json'


def print_compile_stats(stats):
    names = {
        NodeType.Model: 'models',
        NodeType.Test: 'tests',
        NodeType.Archive: 'archives',
        NodeType.Analysis: 'analyses',
        NodeType.Macro: 'macros',
        NodeType.Operation: 'operations',
        NodeType.Seed: 'seed files',
    }

    results = {k: 0 for k in names.keys()}
    results.update(stats)

    stat_line = ", ".join(
        ["{} {}".format(ct, names.get(t)) for t, ct in results.items()])

    logger.info("Found {}".format(stat_line))


def prepend_ctes(model, flat_graph):
    model, _, flat_graph = recursively_prepend_ctes(model, flat_graph)

    return (model, flat_graph)


def recursively_prepend_ctes(model, flat_graph):
    if dbt.flags.STRICT_MODE:
        dbt.contracts.graph.compiled.CompiledNode(**model)
        dbt.contracts.graph.compiled.CompiledGraph(**flat_graph)

    model = model.copy()
    prepend_ctes = OrderedDict()

    if model.get('all_ctes_injected') is True:
        return (model, model.get('extra_ctes').keys(), flat_graph)

    for cte_id in model.get('extra_ctes', {}):
        cte_to_add = flat_graph.get('nodes').get(cte_id)
        cte_to_add, new_prepend_ctes, flat_graph = recursively_prepend_ctes(
            cte_to_add, flat_graph)

        prepend_ctes.update(new_prepend_ctes)
        new_cte_name = '__dbt__CTE__{}'.format(cte_to_add.get('name'))
        prepend_ctes[cte_id] = ' {} as (\n{}\n)'.format(
            new_cte_name,
            cte_to_add.get('compiled_sql'))

    model['extra_ctes_injected'] = True
    model['extra_ctes'] = prepend_ctes
    model['injected_sql'] = inject_ctes_into_sql(
        model.get('compiled_sql'),
        prepend_ctes)

    flat_graph['nodes'][model.get('unique_id')] = model

    return (model, prepend_ctes, flat_graph)


def inject_ctes_into_sql(sql, ctes):
    """
    `ctes` is a dict of CTEs in the form:

      {
        "cte_id_1": "__dbt__CTE__ephemeral as (select * from table)",
        "cte_id_2": "__dbt__CTE__events as (select id, type from events)"
      }

    Given `sql` like:

      "with internal_cte as (select * from sessions)
       select * from internal_cte"

    This will spit out:

      "with __dbt__CTE__ephemeral as (select * from table),
            __dbt__CTE__events as (select id, type from events),
            with internal_cte as (select * from sessions)
       select * from internal_cte"

    (Whitespace enhanced for readability.)
    """
    if len(ctes) == 0:
        return sql

    parsed_stmts = sqlparse.parse(sql)
    parsed = parsed_stmts[0]

    with_stmt = None
    for token in parsed.tokens:
        if token.is_keyword and token.normalized == 'WITH':
            with_stmt = token
            break

    if with_stmt is None:
        # no with stmt, add one, and inject CTEs right at the beginning
        first_token = parsed.token_first()
        with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
        parsed.insert_before(first_token, with_stmt)
    else:
        # stmt exists, add a comma (which will come after injected CTEs)
        trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ',')
        parsed.insert_after(with_stmt, trailing_comma)

    parsed.insert_after(
        with_stmt,
        sqlparse.sql.Token(sqlparse.tokens.Keyword, ", ".join(ctes.values())))

    return dbt.compat.to_string(parsed)


class Compiler(object):
    def __init__(self, project):
        self.project = project

    def initialize(self):
        dbt.clients.system.make_directory(self.project['target-path'])
        dbt.clients.system.make_directory(self.project['modules-path'])

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        write_file(target_path, payload)

        return target_path

    def compile_node(self, node, flat_graph):
        logger.debug("Compiling {}".format(node.get('unique_id')))

        compiled_node = node.copy()
        compiled_node.update({
            'compiled': False,
            'compiled_sql': None,
            'extra_ctes_injected': False,
            'extra_ctes': OrderedDict(),
            'injected_sql': None,
        })

        context = dbt.context.runtime.generate(
            compiled_node, self.project, flat_graph)

        compiled_node['compiled_sql'] = dbt.clients.jinja.get_rendered(
            node.get('raw_sql'),
            context,
            node)

        compiled_node['compiled'] = True

        injected_node, _ = prepend_ctes(compiled_node, flat_graph)

        if compiled_node.get('resource_type') in [NodeType.Test,
                                                  NodeType.Analysis,
                                                  NodeType.Operation]:
            # data tests get wrapped in count(*)
            # TODO : move this somewhere more reasonable
            if 'data' in injected_node['tags'] and \
               is_type(injected_node, NodeType.Test):
                injected_node['wrapped_sql'] = (
                    "select count(*) from (\n{test_sql}\n) sbq").format(
                        test_sql=injected_node['injected_sql'])
            else:
                # don't wrap schema tests or analyses.
                injected_node['wrapped_sql'] = injected_node.get(
                    'injected_sql')

        elif is_type(injected_node, NodeType.Archive):
            # unfortunately we do everything automagically for
            # archives. in the future it'd be nice to generate
            # the SQL at the parser level.
            pass

        elif(is_type(injected_node, NodeType.Model) and
             get_materialization(injected_node) == 'ephemeral'):
            pass

        else:
            injected_node['wrapped_sql'] = None

        return injected_node

    def write_manifest_file(self, manifest):
        """Write the manifest file to disk.

        manifest should be a ParsedManifest.
        """
        filename = manifest_file_name
        manifest_path = os.path.join(self.project['target-path'], filename)
        write_file(manifest_path, json.dumps(manifest.serialize()))

    def write_graph_file(self, linker):
        filename = graph_file_name
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def link_node(self, linker, node, flat_graph):
        linker.add_node(node.get('unique_id'))

        linker.update_node_data(
            node.get('unique_id'),
            node)

        for dependency in node.get('depends_on', {}).get('nodes'):
            if flat_graph.get('nodes').get(dependency):
                linker.dependency(
                    node.get('unique_id'),
                    (flat_graph.get('nodes')
                               .get(dependency)
                               .get('unique_id')))

            else:
                dbt.exceptions.dependency_not_found(node, dependency)

    def link_graph(self, linker, flat_graph):
        linked_graph = {
            'nodes': {},
            'macros': flat_graph.get('macros')
        }

        for name, node in flat_graph.get('nodes').items():
            self.link_node(linker, node, flat_graph)
            linked_graph['nodes'][name] = node

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

        return linked_graph

    def get_all_projects(self):
        root_project = self.project.cfg
        all_projects = {root_project.get('name'): root_project}
        dependency_projects = dbt.utils.dependency_projects(self.project)

        for project in dependency_projects:
            name = project.cfg.get('name', 'unknown')
            all_projects[name] = project.cfg

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        return all_projects

    def _check_resource_uniqueness(cls, flat_graph):
        nodes = flat_graph['nodes']
        names_resources = {}
        alias_resources = {}

        for resource, node in nodes.items():
            if node.get('resource_type') not in NodeType.refable():
                continue

            name = node['name']
            alias = "{}.{}".format(node['schema'], node['alias'])

            existing_node = names_resources.get(name)
            if existing_node is not None:
                dbt.exceptions.raise_duplicate_resource_name(
                        existing_node, node)

            existing_alias = alias_resources.get(alias)
            if existing_alias is not None:
                dbt.exceptions.raise_ambiguous_alias(
                        existing_alias, node)

            names_resources[name] = node
            alias_resources[alias] = node

    def compile(self):
        linker = Linker()

        root_project = self.project.cfg
        all_projects = self.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(root_project, all_projects)

        self.write_manifest_file(manifest)

        flat_graph = manifest.to_flat_graph()

        self._check_resource_uniqueness(flat_graph)

        linked_graph = self.link_graph(linker, flat_graph)

        stats = defaultdict(int)

        for node_name, node in itertools.chain(
                linked_graph.get('nodes').items(),
                linked_graph.get('macros').items()):
            stats[node.get('resource_type')] += 1

        self.write_graph_file(linker)
        print_compile_stats(stats)

        return linked_graph, linker
