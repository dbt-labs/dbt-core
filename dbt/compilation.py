import os
import fnmatch
import jinja2
from collections import defaultdict
import time
import sqlparse

import dbt.project
import dbt.utils

from dbt.model import Model, NodeType
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, \
     split_path, This, Var, compiler_error

from dbt.linker import Linker
from dbt.runtime import RuntimeContext

import dbt.compat
import dbt.contracts.graph.compiled
import dbt.contracts.graph.parsed
import dbt.contracts.project
import dbt.flags
import dbt.parser
import dbt.templates

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger

CompilableEntities = [
    "models", "data tests", "schema tests", "archives", "analyses"
]

graph_file_name = 'graph.gpickle'


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


def compile_string(string, ctx):
    try:
        env = jinja2.Environment()
        template = env.from_string(dbt.compat.to_string(string), globals=ctx)
        return template.render(ctx)
    except jinja2.exceptions.TemplateSyntaxError as e:
        compiler_error(None, str(e))
    except jinja2.exceptions.UndefinedError as e:
        compiler_error(None, str(e))


def prepend_ctes(model, all_models):
    model, _, all_models = recursively_prepend_ctes(model, all_models)

    return (model, all_models)


def recursively_prepend_ctes(model, all_models):
    if dbt.flags.STRICT_MODE:
        dbt.contracts.graph.compiled.validate_one(model)
        dbt.contracts.graph.compiled.validate(all_models)

    model = model.copy()
    prepend_ctes = []

    if model.get('all_ctes_injected') is True:
        return (model, model.get('extra_cte_ids'), all_models)

    for cte_id in model.get('extra_cte_ids'):
        cte_to_add = all_models.get(cte_id)
        cte_to_add, new_prepend_ctes, all_models = recursively_prepend_ctes(
            cte_to_add, all_models)

        prepend_ctes = new_prepend_ctes + prepend_ctes
        new_cte_name = '__dbt__CTE__{}'.format(cte_to_add.get('name'))
        prepend_ctes.append(' {} as (\n{}\n)'.format(
            new_cte_name,
            cte_to_add.get('compiled_sql')))

    model['extra_ctes_injected'] = True
    model['extra_cte_sql'] = prepend_ctes
    model['injected_sql'] = inject_ctes_into_sql(
        model.get('compiled_sql'),
        model.get('extra_cte_sql'))

    all_models[model.get('unique_id')] = model

    return (model, prepend_ctes, all_models)


def inject_ctes_into_sql(sql, ctes):
    """
    `ctes` is a list of CTEs in the form:

      [ "__dbt__CTE__ephemeral as (select * from table)",
        "__dbt__CTE__events as (select id, type from events)" ]

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
        sqlparse.sql.Token(sqlparse.tokens.Keyword, ", ".join(ctes)))

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

    def get_macros(self, this_project, own_project=None):
        if own_project is None:
            own_project = this_project
        paths = own_project.get('macro-paths', [])
        return Source(this_project, own_project=own_project).get_macros(paths)

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        dbt.compat.write_file(target_path, payload)

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
                compiler_error(
                    model,
                    "ref() takes at most two arguments ({} given)".format(
                        len(args)
                    )
                )

            target_model = dbt.utils.find_model_by_name(
                all_models,
                target_model_name,
                target_model_package)

            if target_model is None:
                compiler_error(
                    model,
                    "Model '{}' depends on model '{}' which was not found."
                    .format(model.get('unique_id'), target_model_name))

            target_model_id = target_model.get('unique_id')

            if target_model.get('config', {}).get('enabled') is False and \
               model.get('config', {}).get('enabled') is True:
                compiler_error(
                    model,
                    "Model '{}' depends on model '{}' which is disabled in "
                    "the project config".format(model.get('unique_id'),
                                                target_model.get('unique_id')))

            model['depends_on'].append(target_model_id)

            if target_model.get('config', {}) \
                           .get('materialized') == 'ephemeral':

                model['extra_cte_ids'].append(target_model_id)
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
                    if m.get('resource_type') == NodeType.Model:
                        logger.info(" - {}".format(m.get('unique_id')))
                raise e

        return wrapped_do_ref

    def get_compiler_context(self, linker, model, models,
                             macro_generator=None):
        context = self.project.context()
        adapter = get_adapter(self.project.run_environment())

        # built-ins
        context['ref'] = self.__ref(context, model, models)
        context['config'] = self.__model_config(model, linker)
        context['this'] = This(
            context['env']['schema'],
            (model.get('name') if dbt.flags.NON_DESTRUCTIVE
             else '{}__dbt_tmp'.format(model.get('name'))),
            model.get('name')
        )
        context['var'] = Var(model, context=context)
        context['target'] = self.project.get_target()

        # these get re-interpolated at runtime!
        context['run_started_at'] = '{{ run_started_at }}'
        context['invocation_id'] = '{{ invocation_id }}'
        context['sql_now'] = adapter.date_function

        if macro_generator is not None:
            for macro_data in macro_generator(context):
                macro = macro_data["macro"]
                macro_name = macro_data["name"]
                project = macro_data["project"]

                if context.get(project.get('name')) is None:
                    context[project.get('name')] = {}

                context.get(project.get('name'), {}) \
                       .update({macro_name: macro})

                if model.get('package_name') == project.get('name'):
                    context.update({macro_name: macro})

        return context

    def get_context(self, linker, model, models):
        runtime = RuntimeContext(model=model)

        context = self.project.context()

        # built-ins
        context['ref'] = self.__ref(context, model, models)
        context['config'] = self.__model_config(model, linker)
        context['this'] = This(
            context['env']['schema'], model.immediate_name, model.name
        )
        context['var'] = Var(model, context=context)
        context['target'] = self.project.get_target()

        # these get re-interpolated at runtime!
        context['run_started_at'] = '{{ run_started_at }}'
        context['invocation_id'] = '{{ invocation_id }}'

        adapter = get_adapter(self.project.run_environment())
        context['sql_now'] = adapter.date_function

        runtime.update_global(context)

        return runtime

    def compile_node(self, linker, node, nodes, macro_generator):
        logger.debug("Compiling {}".format(node.get('unique_id')))
        try:
            compiled_node = node.copy()
            compiled_node.update({
                'compiled': False,
                'compiled_sql': None,
                'extra_ctes_injected': False,
                'extra_cte_ids': [],
                'extra_cte_sql': [],
                'injected_sql': None,
            })

            context = self.get_compiler_context(linker, compiled_node, nodes,
                                                macro_generator)

            env = jinja2.sandbox.SandboxedEnvironment()

            compiled_node['compiled_sql'] = env.from_string(
                node.get('raw_sql')).render(context)

            compiled_node['compiled'] = True
        except jinja2.exceptions.TemplateSyntaxError as e:
            compiler_error(node, str(e))
        except jinja2.exceptions.UndefinedError as e:
            compiler_error(node, str(e))

        return compiled_node

    def write_graph_file(self, linker):
        filename = graph_file_name
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def new_add_cte_to_rendered_query(self, linker, primary_model,
                                      compiled_models):

        fqn_to_model = {tuple(model.fqn): model for model in compiled_models}
        sorted_nodes = linker.as_topological_ordering()

        models_to_add = self.__recursive_add_ctes(linker, primary_model)

        required_ctes = []
        for node in sorted_nodes:

            if node not in fqn_to_model:
                continue

            model = fqn_to_model[node]
            # add these in topological sort order -- significant for CTEs
            if model.is_ephemeral and model in models_to_add:
                required_ctes.append(model)

        query = compiled_models[primary_model]
        if len(required_ctes) == 0:
            return query
        else:
            compiled_query = self.combine_query_with_ctes(
                primary_model, query, required_ctes, compiled_models
            )
            return compiled_query

    def compile_nodes(self, linker, nodes, macro_generator):
        all_projects = self.get_all_projects()

        compiled_nodes = {}
        injected_nodes = {}
        wrapped_nodes = {}
        written_nodes = []

        for name, node in nodes.items():
            compiled_nodes[name] = self.compile_node(linker, node, nodes,
                                                     macro_generator)

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(compiled_nodes)

        for name, node in compiled_nodes.items():
            node, compiled_nodes = prepend_ctes(node, compiled_nodes)
            injected_nodes[name] = node

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(injected_nodes)

        for name, injected_node in injected_nodes.items():
            # now turn model nodes back into the old-style model object for
            # wrapping
            if injected_node.get('resource_type') in [NodeType.Test,
                                                      NodeType.Analysis]:
                # don't wrap tests or analyses.
                injected_node['wrapped_sql'] = injected_node['injected_sql']
                wrapped_nodes[name] = injected_node

            elif injected_node.get('resource_type') == NodeType.Archive:
                # unfortunately we do everything automagically for
                # archives. in the future it'd be nice to generate
                # the SQL at the parser level.
                pass

            else:
                model = Model(
                    self.project,
                    injected_node.get('root_path'),
                    injected_node.get('path'),
                    all_projects.get(injected_node.get('package_name')))

                cfg = injected_node.get('config', {})
                model._config = cfg

                context = self.get_context(linker, model, injected_nodes)

                wrapped_stmt = model.compile(
                    injected_node.get('injected_sql'), self.project, context)

                injected_node['wrapped_sql'] = wrapped_stmt
                wrapped_nodes[name] = injected_node

            build_path = os.path.join('build', injected_node.get('path'))

            if injected_node.get('resource_type') in (NodeType.Model,
                                                      NodeType.Analysis) and \
               injected_node.get('config', {}) \
                            .get('materialized') != 'ephemeral':
                self.__write(build_path, injected_node.get('wrapped_sql'))
                written_nodes.append(injected_node)
                injected_node['build_path'] = build_path

            linker.add_node(injected_node.get('unique_id'))
            project = all_projects[injected_node.get('package_name')]

            linker.update_node_data(
                injected_node.get('unique_id'),
                injected_node)

            for dependency in injected_node.get('depends_on'):
                if compiled_nodes.get(dependency):
                    linker.dependency(
                        injected_node.get('unique_id'),
                        compiled_nodes.get(dependency).get('unique_id'))
                else:
                    compiler_error(
                        model,
                        "dependency {} not found in graph!".format(
                            dependency))

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

        return wrapped_nodes, written_nodes

    def generate_macros(self, all_macros):
        def do_gen(ctx):
            macros = []
            for macro in all_macros:
                new_macros = macro.get_macros(ctx)
                macros.extend(new_macros)
            return macros
        return do_gen

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

    def get_parsed_models(self, root_project, all_projects, macro_generator):
        parsed_models = {}

        for name, project in all_projects.items():
            parsed_models.update(
                dbt.parser.load_and_parse_sql(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('source-paths', []),
                    resource_type=NodeType.Model,
                    macro_generator=macro_generator))

        return parsed_models

    def get_parsed_analyses(self, root_project, all_projects, macro_generator):
        parsed_models = {}

        for name, project in all_projects.items():
            parsed_models.update(
                dbt.parser.load_and_parse_sql(
                    package_name=name,
                    root_project=root_project,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('analysis-paths', []),
                    resource_type=NodeType.Analysis,
                    macro_generator=macro_generator))

        return parsed_models

    def get_parsed_data_tests(self, root_project, all_projects,
                              macro_generator):
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
                    macro_generator=macro_generator,
                    tags=['data']))

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

    def load_all_nodes(self, root_project, all_projects, macro_generator):
        all_nodes = {}

        all_nodes.update(self.get_parsed_models(root_project, all_projects,
                                                macro_generator))
        all_nodes.update(self.get_parsed_analyses(root_project, all_projects,
                                                  macro_generator))
        all_nodes.update(
            self.get_parsed_data_tests(root_project, all_projects,
                                       macro_generator))
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

        all_macros = self.get_macros(this_project=self.project)

        for project in dbt.utils.dependency_projects(self.project):
            all_macros.extend(
                self.get_macros(this_project=self.project, own_project=project)
            )

        macro_generator = self.generate_macros(all_macros)

        all_nodes = self.load_all_nodes(root_project, all_projects,
                                        macro_generator)

        compiled_nodes, written_nodes = self.compile_nodes(linker, all_nodes,
                                                           macro_generator)

        # TODO re-add archives

        self.write_graph_file(linker)

        stats = {}

        for node_name, node in compiled_nodes.items():
            stats[node.get('resource_type')] = stats.get(
                node.get('resource_type'), 0) + 1

        return stats
