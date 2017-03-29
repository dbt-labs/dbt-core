import os
from collections import OrderedDict, defaultdict
import sqlparse

import dbt.project
import dbt.utils
import dbt.include
import dbt.wrapper

from dbt.model import Model
from dbt.utils import This, Var, is_enabled, get_materialization, NodeType, \
    is_type

from dbt.linker import Linker
from dbt.runtime import RuntimeContext

import dbt.compat
import dbt.contracts.graph.compiled
import dbt.contracts.project
import dbt.exceptions
import dbt.flags
import dbt.parser
import dbt.templates

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger

CompilableEntities = [
    "models", "data tests", "schema tests", "archives", "analyses"
]

graph_file_name = 'graph.gpickle'


def recursively_parse_macros_for_node(node, flat_graph, context):
    # this once worked, but is now long dead
    # for unique_id in node.get('depends_on', {}).get('macros'):
    # TODO: make it so that we only parse the necessary macros for any node.

    for unique_id, macro in flat_graph.get('macros').items():
        if macro is None:
            dbt.exceptions.macro_not_found(node, unique_id)

        name = macro.get('name')
        package_name = macro.get('package_name')

        if context.get(package_name, {}).get(name) is not None:
            # we've already re-parsed this macro and added it to
            # the context.
            continue

        reparsed = dbt.parser.parse_macro_file(
            macro_file_path=macro.get('path'),
            macro_file_contents=macro.get('raw_sql'),
            root_path=macro.get('root_path'),
            package_name=package_name,
            context=context)

        for unique_id, macro in reparsed.items():
            macro_map = {macro.get('name'): macro.get('parsed_macro')}

            if context.get(package_name) is None:
                context[package_name] = {}

            context.get(package_name, {}) \
                   .update(macro_map)

            if package_name in (node.get('package_name'),
                                dbt.include.GLOBAL_PROJECT_NAME):
                context.update(macro_map)

    return context


def compile_and_print_status(project, args):
    compiler = Compiler(project, args)
    compiler.initialize()
    names = {
        NodeType.Model: 'models',
        NodeType.Test: 'tests',
        NodeType.Archive: 'archives',
        NodeType.Analysis: 'analyses',
    }

    results = {
        NodeType.Model: 0,
        NodeType.Test: 0,
        NodeType.Archive: 0,
        NodeType.Analysis: 0,
    }

    results.update(compiler.compile())

    stat_line = ", ".join(
        ["{} {}".format(ct, names.get(t)) for t, ct in results.items()])

    logger.info("Compiled {}".format(stat_line))


def prepend_ctes(model, flat_graph):
    model, _, flat_graph = recursively_prepend_ctes(model, flat_graph)

    return (model, flat_graph)


def recursively_prepend_ctes(model, flat_graph):
    if dbt.flags.STRICT_MODE:
        dbt.contracts.graph.compiled.validate_node(model)
        dbt.contracts.graph.compiled.validate(flat_graph)

    model = model.copy()
    prepend_ctes = OrderedDict()

    if model.get('all_ctes_injected') is True:
        return (model, model.get('extra_ctes').keys(), flat_graph)

    for cte_id in model.get('extra_ctes').keys():
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
        model.get('extra_ctes'))

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
    def __init__(self, project, args):
        self.project = project
        self.args = args
        self.parsed_models = None

    def initialize(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        dbt.compat.write_file(target_path, payload)

        return target_path

    def __model_config(self, model, linker):
        def do_config(*args, **kwargs):
            return ''

        return do_config

    def __ref(self, ctx, model, all_models):
        schema = ctx.get('env', {}).get('schema')

        def do_ref(*args):
            target_model_name = None
            target_model_package = None

            if len(args) == 1:
                target_model_name = args[0]
            elif len(args) == 2:
                target_model_package, target_model_name = args
            else:
                dbt.exceptions.ref_invalid_args(model, args)

            target_model = dbt.utils.find_model_by_name(
                all_models,
                target_model_name,
                target_model_package)

            if target_model is None:
                dbt.exceptions.ref_target_not_found(model, target_model_name)

            if is_enabled(model) and not is_enabled(target_model):
                dbt.exceptions.ref_disabled_dependency(model, target_model)

            target_model_id = target_model.get('unique_id')

            if target_model_id not in model['depends_on']['nodes']:
                model['depends_on']['nodes'].append(target_model_id)

            if get_materialization(target_model) == 'ephemeral':
                model['extra_ctes'][target_model_id] = None
                return '__dbt__CTE__{}'.format(target_model.get('name'))
            else:
                return '"{}"."{}"'.format(schema, target_model.get('name'))

        def wrapped_do_ref(*args):
            try:
                return do_ref(*args)
            except RuntimeError as e:
                logger.info("Compiler error in {}".format(model.get('path')))
                logger.info("Enabled models:")
                for n, m in all_models.items():
                    if is_type(m, NodeType.Model):
                        logger.info(" - {}".format(m.get('unique_id')))
                raise e

        return wrapped_do_ref

    def get_compiler_context(self, linker, model, flat_graph):
        context = self.project.context()
        adapter = get_adapter(self.project.run_environment())

        if dbt.flags.NON_DESTRUCTIVE or \
                get_materialization(model) == 'incremental':
            table_name = model.get('name')
        else:
            table_name = '{}__dbt_tmp'.format(model.get('name'))

        # built-ins
        context['ref'] = self.__ref(context, model, flat_graph)
        context['config'] = self.__model_config(model, linker)
        context['this'] = This(
            context['env']['schema'],
            table_name,
            model.get('name')
        )
        context['var'] = Var(model, context=context)
        context['target'] = self.project.get_target()

        # these get re-interpolated at runtime!
        context['run_started_at'] = '{{ run_started_at }}'
        context['invocation_id'] = '{{ invocation_id }}'
        context['sql_now'] = adapter.date_function()

        context = recursively_parse_macros_for_node(
            model, flat_graph, context)

        return context

    def compile_node(self, linker, node, flat_graph):
        logger.debug("Compiling {}".format(node.get('unique_id')))

        compiled_node = node.copy()
        compiled_node.update({
            'compiled': False,
            'compiled_sql': None,
            'extra_ctes_injected': False,
            'extra_ctes': OrderedDict(),
            'injected_sql': None,
        })

        context = self.get_compiler_context(linker, compiled_node, flat_graph)

        compiled_node['compiled_sql'] = dbt.clients.jinja.get_rendered(
            node.get('raw_sql'),
            context,
            node)

        compiled_node['compiled'] = True

        return compiled_node

    def write_graph_file(self, linker):
        filename = graph_file_name
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def compile_graph(self, linker, flat_graph):
        all_projects = self.get_all_projects()

        compiled_graph = {
            'nodes': {},
            'macros': {},
        }
        injected_graph = {
            'nodes': {},
            'macros': {},
        }
        wrapped_graph = {
            'nodes': {},
            'macros': {},
        }
        written_nodes = []

        for name, node in flat_graph.get('nodes').items():
            compiled_graph['nodes'][name] = \
                self.compile_node(linker, node, flat_graph)

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(compiled_graph)

        for name, node in compiled_graph.get('nodes').items():
            node, compiled_graph = prepend_ctes(node, compiled_graph)
            injected_graph['nodes'][name] = node

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(injected_graph)

        for name, injected_node in injected_graph.get('nodes').items():
            # now turn model nodes back into the old-style model object for
            # wrapping
            if injected_node.get('resource_type') in [NodeType.Test,
                                                      NodeType.Analysis]:
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

                wrapped_graph['nodes'][name] = injected_node

            elif is_type(injected_node, NodeType.Archive):
                # unfortunately we do everything automagically for
                # archives. in the future it'd be nice to generate
                # the SQL at the parser level.
                pass

            else:

                context = self.get_compiler_context(linker, injected_node, injected_graph)
                wrapped_stmt = dbt.wrapper.wrap(
                    injected_node,
                    self.project,
                    context,
                    flat_graph)

                injected_node['wrapped_sql'] = wrapped_stmt
                wrapped_graph['nodes'][name] = injected_node

            build_path = os.path.join('build',
                                      injected_node.get('package_name'),
                                      injected_node.get('path'))

            if injected_node.get('resource_type') in (NodeType.Model,
                                                      NodeType.Analysis,
                                                      NodeType.Test) and \
               get_materialization(injected_node) != 'ephemeral':
                written_path = self.__write(
                    build_path, injected_node.get('wrapped_sql'))
                written_nodes.append(injected_node)
                injected_node['build_path'] = written_path

            linker.add_node(injected_node.get('unique_id'))

            linker.update_node_data(
                injected_node.get('unique_id'),
                injected_node)

            for dependency in injected_node.get('depends_on', {}).get('nodes'):
                if compiled_graph.get('nodes').get(dependency):
                    linker.dependency(
                        injected_node.get('unique_id'),
                        (compiled_graph.get('nodes')
                                       .get(dependency)
                                       .get('unique_id')))
                else:
                    dbt.exceptions.dependency_not_found(model, dependency)

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

        return wrapped_graph, written_nodes

    def get_all_projects(self):
        root_project = self.project.cfg
        all_projects = {root_project.get('name'): root_project}
        dependency_projects = dbt.utils.dependency_projects(self.project)

        for project in dependency_projects:
            name = project.cfg.get('name', 'unknown')
            all_projects[name] = project.cfg

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.validate_list(all_projects)

        return all_projects

    def get_parsed_macros(self, root_project, all_projects):
        parsed_macros = {}

        for name, project in all_projects.items():
            parsed_macros.update(
                dbt.parser.load_and_parse_macros(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('macro-paths', []),
                    resource_type=NodeType.Macro))

        return parsed_macros

    def get_parsed_models(self, root_project, all_projects):
        parsed_models = {}

        for name, project in all_projects.items():
            parsed_models.update(
                dbt.parser.load_and_parse_sql(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('source-paths', []),
                    resource_type=NodeType.Model))

        return parsed_models

    def get_parsed_analyses(self, root_project, all_projects):
        parsed_models = {}

        for name, project in all_projects.items():
            parsed_models.update(
                dbt.parser.load_and_parse_sql(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('analysis-paths', []),
                    resource_type=NodeType.Analysis))

        return parsed_models

    def get_parsed_data_tests(self, root_project, all_projects):
        parsed_tests = {}

        for name, project in all_projects.items():
            parsed_tests.update(
                dbt.parser.load_and_parse_sql(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('test-paths', []),
                    resource_type=NodeType.Test,
                    tags={'data'}))

        return parsed_tests

    def get_parsed_schema_tests(self, root_project, all_projects):
        parsed_tests = {}

        for name, project in all_projects.items():
            parsed_tests.update(
                dbt.parser.load_and_parse_yml(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('source-paths', [])))

        return parsed_tests

    def load_all_macros(self, root_project, all_projects):
        return self.get_parsed_macros(root_project, all_projects)

    def load_all_nodes(self, root_project, all_projects):
        all_nodes = {}

        all_nodes.update(self.get_parsed_models(root_project, all_projects))
        all_nodes.update(self.get_parsed_analyses(root_project, all_projects))
        all_nodes.update(
            self.get_parsed_data_tests(root_project, all_projects))
        all_nodes.update(
            self.get_parsed_schema_tests(root_project, all_projects))
        all_nodes.update(
            dbt.parser.parse_archives_from_projects(root_project,
                                                    all_projects))

        return all_nodes

    def compile(self):
        linker = Linker()

        root_project = self.project.cfg
        all_projects = self.get_all_projects()

        all_macros = self.load_all_macros(root_project, all_projects)
        all_nodes = self.load_all_nodes(root_project, all_projects)

        flat_graph = {
            'nodes': all_nodes,
            'macros': all_macros
        }

        compiled_graph, written_nodes = self.compile_graph(linker, flat_graph)

        self.write_graph_file(linker)

        stats = defaultdict(int)

        for node_name, node in compiled_graph.get('nodes').items():
            stats[node.get('resource_type')] += 1

        for node_name, node in compiled_graph.get('macros').items():
            stats[node.get('resource_type')] += 1

        return stats
