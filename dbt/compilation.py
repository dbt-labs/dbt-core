import os
import fnmatch
import jinja2
from collections import defaultdict
import time
import sqlparse

import dbt.project
import dbt.utils

from dbt.model import Model
from dbt.source import Source
from dbt.utils import find_model_by_fqn, find_model_by_name, \
     split_path, This, Var, compiler_error, to_string

from dbt.linker import Linker
from dbt.runtime import RuntimeContext

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

graph_file_name = 'graph.yml'


def compile_string(string, ctx):
    try:
        env = jinja2.Environment()
        template = env.from_string(str(string), globals=ctx)
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

    if model.get('all_ctes_injected') == True:
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

    return str(parsed)


class Compiler(object):
    def __init__(self, project, args):
        self.project = project
        self.args = args
        self.parsed_models = None

        self.macro_generator = None

    def initialize(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def model_sources(self, this_project, own_project=None):
        if own_project is None:
            own_project = this_project

        paths = own_project.get('source-paths', [])
        return Source(
            this_project,
            own_project=own_project
        ).get_models(paths)

    def get_macros(self, this_project, own_project=None):
        if own_project is None:
            own_project = this_project
        paths = own_project.get('macro-paths', [])
        return Source(this_project, own_project=own_project).get_macros(paths)

    def get_archives(self, project):
        return Source(
            project,
            own_project=project
        ).get_archives()

    def project_schemas(self, project):
        source_paths = project.get('source-paths', [])
        return Source(project).get_schemas(source_paths)

    def project_tests(self, project):
        source_paths = project.get('test-paths', [])
        return Source(project).get_tests(source_paths)

    def analysis_sources(self, project):
        paths = project.get('analysis-paths', [])
        return Source(project).get_analyses(paths)

    def validate_models_unique(self, models, error_type):
        found_models = defaultdict(list)
        for model in models:
            found_models[model.name].append(model)
        for model_name, model_list in found_models.items():
            if len(model_list) > 1:
                models_str = "\n  - ".join(
                    [str(model) for model in model_list])

                error_msg = "Found {} models with the same name.\n" \
                            "  Name='{}'\n" \
                            "  - {}".format(
                                    len(model_list), model_name, models_str
                            )

                error_type(model_list[0], error_msg)

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        with open(target_path, 'w') as f:
            f.write(to_string(payload))

    def __model_config(self, model, linker):
        def do_config(*args, **kwargs):
            return ''

        return do_config

    def model_can_reference(self, src_model, other_model):
        """
        returns True if the src_model can reference the other_model. Models
        can access other models in their package and dependency models, but
        a dependency model cannot access models "up" the dependency chain.
        """

        # hack for now b/c we don't support recursive dependencies
        return (
            other_model.own_project['name'] == src_model.own_project['name'] or
            src_model.own_project['name'] == src_model.project['name']
        )

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

            if target_model.get('config', {}) \
                           .get('enabled') == False:
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
                for n,m in all_models.items():
                    logger.info(" - {}".format(".".join(m.get('fqn'))))
                raise e

        return wrapped_do_ref

    def get_compiler_context(self, linker, model, models):
        runtime = RuntimeContext(model=model)

        context = self.project.context()

        # built-ins
        context['ref'] = self.__ref(context, model, models)
        context['config'] = self.__model_config(model, linker)
        #context['this'] = This(
        #    context['env']['schema'], model.immediate_name, model.name
        #)
        context['var'] = Var(model, context=context)
        context['target'] = self.project.get_target()

        # these get re-interpolated at runtime!
        context['run_started_at'] = '{{ run_started_at }}'
        context['invocation_id'] = '{{ invocation_id }}'

        adapter = get_adapter(self.project.run_environment())
        context['sql_now'] = adapter.date_function

        runtime.update_global(context)

        # add in macros (can we cache these somehow?)
        for macro_data in self.macro_generator(context):
            macro = macro_data["macro"]
            macro_name = macro_data["name"]
            project = macro_data["project"]

            runtime.update_package(project['name'], {macro_name: macro})

            if project['name'] == self.project['name']:
                runtime.update_global({macro_name: macro})

        return runtime

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

        # add in macros (can we cache these somehow?)
        for macro_data in self.macro_generator(context):
            macro = macro_data["macro"]
            macro_name = macro_data["name"]
            project = macro_data["project"]

            runtime.update_package(project['name'], {macro_name: macro})

            if project['name'] == self.project['name']:
                runtime.update_global({macro_name: macro})

        return runtime

    def compile_model(self, linker, model, models):
        try:
            compiled_model = model.copy()
            compiled_model.update({
                'compiled': False,
                'compiled_sql': None,
                'extra_ctes_injected': False,
                'extra_cte_ids': [],
                'extra_cte_sql': [],
                'injected_sql': None,
            })

            context = self.get_compiler_context(linker, compiled_model, models)

            env = jinja2.sandbox.SandboxedEnvironment()

            compiled_model['compiled_sql'] = env.from_string(
                model.get('raw_sql')).render(context)

            compiled_model['compiled'] = True
        except jinja2.exceptions.TemplateSyntaxError as e:
            compiler_error(model, str(e))
        except jinja2.exceptions.UndefinedError as e:
            compiler_error(model, str(e))

        return compiled_model

    def write_graph_file(self, linker):
        filename = graph_file_name
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def combine_query_with_ctes(self, model, query, ctes, compiled_models):
        parsed_stmts = sqlparse.parse(query)
        if len(parsed_stmts) != 1:
            raise RuntimeError(
                "unexpectedly parsed {} queries from model "
                "{}".format(len(parsed_stmts), model)
            )

        parsed = parsed_stmts[0]

        with_stmt = None
        for token in parsed.tokens:
            if token.is_keyword and token.normalized == 'WITH':
                with_stmt = token
                break

        if with_stmt is None:
            # no with stmt, add one!
            first_token = parsed.token_first()
            with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
            parsed.insert_before(first_token, with_stmt)
        else:
            # stmt exists, add a comma (which will come after our injected
            # CTE(s) )
            trailing_comma = sqlparse.sql.Token(
                sqlparse.tokens.Punctuation, ','
            )
            parsed.insert_after(with_stmt, trailing_comma)

        cte_mapping = [
            (model.cte_name, compiled_models[model]) for model in ctes
        ]

        # these newlines are important -- comments could otherwise interfere
        # w/ query
        cte_stmts = [
            " {} as (\n{}\n)".format(name, contents)
            for (name, contents) in cte_mapping
        ]

        cte_text = sqlparse.sql.Token(
            sqlparse.tokens.Keyword, ", ".join(cte_stmts)
        )
        parsed.insert_after(with_stmt, cte_text)

        return str(parsed)

    def __recursive_add_ctes(self, linker, model):
        if model not in linker.cte_map:
            return set()

        models_to_add = linker.cte_map[model]
        recursive_models = [
            self.__recursive_add_ctes(linker, m) for m in models_to_add
        ]

        for recursive_model_set in recursive_models:
            models_to_add = models_to_add | recursive_model_set

        return models_to_add

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


    def add_cte_to_rendered_query(
            self, linker, primary_model, compiled_models
    ):
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

    def remove_node_from_graph(self, linker, model, models):
        # remove the node
        children = linker.remove_node(tuple(model.fqn))

        # check if we bricked the graph. if so: throw compilation error
        for child in children:
            other_model = find_model_by_fqn(models, child)

            if other_model.is_enabled:
                this_fqn = ".".join(model.fqn)
                that_fqn = ".".join(other_model.fqn)
                compiler_error(
                    model,
                    "Model '{}' depends on model '{}' which is "
                    "disabled".format(that_fqn, this_fqn)
                )

    def compile_models(self, linker, models):
        all_projects = {'root': self.project}
        dependency_projects = dbt.utils.dependency_projects(self.project)

        for project in dependency_projects:
            name = project.cfg.get('name', 'unknown')
            all_projects[name] = project

        compiled_models = {}
        injected_models = {}
        wrapped_models = {}
        written_models = []

        for name, model in models.items():
            compiled_models[name] = self.compile_model(linker, model, models)

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(compiled_models)

        for name, model in compiled_models.items():
            model, compiled_models = prepend_ctes(model, compiled_models)
            injected_models[name] = model

        if dbt.flags.STRICT_MODE:
            dbt.contracts.graph.compiled.validate(injected_models)

        for name, injected_model in injected_models.items():
            # now turn a model back into the old-style model object
            model = Model(
                self.project,
                injected_model.get('root_path'),
                injected_model.get('path'),
                all_projects[injected_model.get('package_name')])

            model._config = injected_model.get('config', {})

            context = self.get_context(linker, model, injected_models)

            wrapped_stmt = model.compile(
                injected_model.get('injected_sql'), self.project, context)

            injected_model['wrapped_sql'] = wrapped_stmt
            wrapped_model = injected_model
            wrapped_models[name] = wrapped_model

            build_path = os.path.join('build', injected_model.get('path'))
            if injected_model.get('config', {}) \
                             .get('materialized') != 'ephemeral':
                self.__write(build_path, wrapped_stmt)
                written_models.append(model)

            linker.add_node(tuple(wrapped_model.get('fqn')))
            project = all_projects[wrapped_model.get('package_name')]

            linker.update_node_data(
                tuple(wrapped_model.get('fqn')),
                {
                    'materialized': (wrapped_model.get('config', {})
                                                  .get('materialized')),
                    'dbt_run_type': dbt.model.NodeType.Model,
                    'enabled': (wrapped_model.get('config', {})
                                             .get('enabled')),
                    'build_path': os.path.join(project['target-path'],
                                               build_path),
                    'name': wrapped_model.get('name'),
                    'tmp_name': model.tmp_name(),
                    'project_name': project.cfg.get('name')
                })

            for dependency in wrapped_model.get('depends_on'):
                if compiled_models.get(dependency):
                    linker.dependency(
                        tuple(wrapped_model.get('fqn')),
                        tuple(compiled_models.get(dependency).get('fqn')))
                else:
                    compiler_error(
                        model,
                        "dependency {} not found in graph!".format(
                            dependency))

        return compiled_models, written_models

    def compile_analyses(self, linker, compiled_models):
        analyses = self.analysis_sources(self.project)
        compiled_analyses = {
            analysis: self.compile_model(
                linker, analysis, compiled_models
            ) for analysis in analyses
        }

        written_analyses = []
        referenceable_models = {}
        referenceable_models.update(compiled_models)
        referenceable_models.update(compiled_analyses)
        for analysis in analyses:
            injected_stmt = self.add_cte_to_rendered_query(
                linker,
                analysis,
                referenceable_models
            )

            serialized = analysis.serialize()
            linker.update_node_data(tuple(analysis.fqn), serialized)

            build_path = analysis.build_path()
            self.__write(build_path, injected_stmt)
            written_analyses.append(analysis)

        return written_analyses

    def get_local_and_package_sources(self, project, source_getter):
        all_sources = []

        all_sources.extend(source_getter(project))

        for package in dbt.utils.dependency_projects(project):
            all_sources.extend(source_getter(package))

        return all_sources

    def compile_schema_tests(self, linker, models):
        all_schema_specs = self.get_local_and_package_sources(
                self.project,
                self.project_schemas
        )

        schema_tests = []

        for schema in all_schema_specs:
            # compiling a SchemaFile returns >= 0 SchemaTest models
            try:
                schema_tests.extend(schema.compile())
            except RuntimeError as e:
                logger.info("\n" + str(e))
                schema_test_path = schema.filepath
                logger.info("Skipping compilation for {}...\n"
                            .format(schema_test_path))

        written_tests = []
        for schema_test in schema_tests:
            # show a warning if the model being tested doesn't exist
            try:
                source_model = find_model_by_name(models,
                                                  schema_test.model_name,
                                                  None)
            except RuntimeError as e:
                dbt.utils.compiler_warning(schema_test, str(e))
                continue

            serialized = schema_test.serialize()

            model_node = tuple(source_model.get('fqn'))
            test_node = tuple(schema_test.fqn)

            linker.dependency(test_node, model_node)
            linker.update_node_data(test_node, serialized)

            query = schema_test.render()
            self.__write(schema_test.build_path(), query)
            written_tests.append(schema_test)

        return written_tests

    def compile_data_tests(self, linker, models):
        tests = self.get_local_and_package_sources(
                self.project,
                self.project_tests
        )

        written_tests = []
        for data_test in tests:
            serialized = data_test.serialize()
            linker.update_node_data(tuple(data_test.fqn), serialized)
            query = self.compile_model(linker, data_test, models)
            wrapped = data_test.render(query)
            self.__write(data_test.build_path(), wrapped)
            written_tests.append(data_test)

        return written_tests

    def generate_macros(self, all_macros):
        def do_gen(ctx):
            macros = []
            for macro in all_macros:
                new_macros = macro.get_macros(ctx)
                macros.extend(new_macros)
            return macros
        return do_gen

    def compile_archives(self, linker, compiled_models):
        all_archives = self.get_archives(self.project)

        for archive in all_archives:
            sql = archive.compile()
            fqn = tuple(archive.fqn)
            linker.update_node_data(fqn, archive.serialize())
            self.__write(archive.build_path(), sql)

        return all_archives

    def get_models(self):
        all_models = self.model_sources(this_project=self.project)
        for project in dbt.utils.dependency_projects(self.project):
            all_models.extend(
                self.model_sources(
                    this_project=self.project, own_project=project
                )
            )

        return all_models

    def get_all_projects(self):
        root_project = self.project.cfg
        all_projects = {'root': root_project}
        dependency_projects = dbt.utils.dependency_projects(self.project)

        for project in dependency_projects:
            name = project.cfg.get('name', 'unknown')
            all_projects[name] = project.cfg

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.validate_list(all_projects)

        return all_projects


    def get_parsed_models(self, root_project, all_projects):
        parsed_models = {}

        for name, project in all_projects.items():
            parsed_models.update(
                dbt.parser.load_and_parse_models(
                    package_name=name,
                    all_projects=all_projects,
                    root_dir=project.get('project-root'),
                    relative_dirs=project.get('source-paths', [])))

        return parsed_models

    def compile(self):
        linker = Linker()

        root_project = self.project.cfg
        all_projects = self.get_all_projects()

        parsed_models = self.get_parsed_models(root_project, all_projects)

        all_macros = self.get_macros(this_project=self.project)

        for project in dbt.utils.dependency_projects(self.project):
            all_macros.extend(
                self.get_macros(this_project=self.project, own_project=project)
            )

        self.macro_generator = self.generate_macros(all_macros)

        compiled_models, written_models = self.compile_models(
            linker, parsed_models
        )

        compilers = {
            'schema tests': self.compile_schema_tests,
            'data tests': self.compile_data_tests,
            'archives': self.compile_archives,
            'analyses': self.compile_analyses
        }

        compiled = {
            'models': written_models
        }

        for (compile_type, compiler_f) in compilers.items():
            newly_compiled = compiler_f(linker, compiled_models)
            compiled[compile_type] = newly_compiled

        self.validate_models_unique(
            compiled['models'],
            dbt.utils.compiler_error
        )

        self.validate_models_unique(
            compiled['data tests'],
            dbt.utils.compiler_warning
        )

        self.validate_models_unique(
            compiled['schema tests'],
            dbt.utils.compiler_warning
        )

        self.write_graph_file(linker)

        stats = {ttype: len(m) for (ttype, m) in compiled.items()}
        return stats
