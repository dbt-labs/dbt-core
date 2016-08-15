
import os
import fnmatch
import jinja2
from collections import defaultdict
import dbt.project
from dbt.source import Source
from dbt.utils import find_model_by_name
import sqlparse

import networkx as nx

class Linker(object):
    def __init__(self):
        self.graph = nx.DiGraph()
        self.cte_map = defaultdict(set)

    def nodes(self):
        return self.graph.nodes()

    def get_node(self, node):
        return self.graph.node[node]

    def as_topological_ordering(self, limit_to=None):
        try:
            return nx.topological_sort(self.graph, nbunch=limit_to)
        except KeyError as e:
            raise RuntimeError("Couldn't find model '{}' -- does it exist or is it diabled?".format(e))

    def as_dependency_list(self, limit_to=None):
        """returns a list of list of nodes, eg. [[0,1], [2], [4,5,6]]. Each element contains nodes whose
        dependenices are subsumed by the union of all lists before it. In this way, all nodes in list `i`
        can be run simultaneously assuming that all lists before list `i` have been completed"""

        if limit_to is None:
            graph_nodes = set(self.graph.nodes())
        else:
            graph_nodes = set()
            for node in limit_to:
                graph_nodes.add(node)
                if node in self.graph:
                    graph_nodes.update(nx.descendants(self.graph, node))
                else:
                    raise RuntimeError("Couldn't find model '{}' -- does it exist or is it diabled?".format(node))

        depth_nodes = defaultdict(list)

        for node in graph_nodes:
            num_ancestors = len(nx.ancestors(self.graph, node))
            depth_nodes[num_ancestors].append(node)

        dependency_list = []
        for depth in sorted(depth_nodes.keys()):
            dependency_list.append(depth_nodes[depth])

        return dependency_list

    def inject_cte(self, source, cte_model):
        self.cte_map[source].add(cte_model)

    def is_child_of(self, nodes, target_node):
        "returns True if node is a child of a node in nodes. Otherwise, False"
        node_span = set()
        for node in nodes:
            node_span.add(node)
            for child in nx.descendants(self.graph, node):
                node_span.add(child)

        return target_node in node_span

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node2, node1)

    def add_node(self, node, data):
        self.graph.add_node(node, data)

    def write_graph(self, outfile):
        nx.write_yaml(self.graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_yaml(infile)

class Compiler(object):
    def __init__(self, project, create_template_class):
        self.project = project
        self.create_template = create_template_class()

    def initialize(self):
        if not os.path.exists(self.project['target-path']):
            os.makedirs(self.project['target-path'])

        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

    def dependency_projects(self):
        for obj in os.listdir(self.project['modules-path']):
            full_obj = os.path.join(self.project['modules-path'], obj)
            if os.path.isdir(full_obj):
                project = dbt.project.read_project(os.path.join(full_obj, 'dbt_project.yml'))
                yield project

    def model_sources(self, this_project, own_project=None):
        "source_key is a dbt config key like source-paths or analysis-paths"

        if own_project is None:
            own_project = this_project

        paths = this_project.get('source-paths', [])
        if self.create_template.label == 'build':
            return Source(this_project, own_project=own_project).get_models(paths)
        elif self.create_template.label == 'test':
            return Source(this_project, own_project=own_project).get_test_models(paths)
        else:
            raise RuntimeError("unexpected create template type: '{}'".format(self.create_template.label))

    def analysis_sources(self, project):
        "source_key is a dbt config key like source-paths or analysis-paths"
        paths = project.get('analysis-paths', [])
        return Source(project).get_analyses(paths)

    def validate_models_unique(self, models):
        found_models = defaultdict(list)
        for model in models:
            found_models[model.name].append(model)
        for model_name, model_list in found_models.items():
            if len(model_list) > 1:
                models_str = "\n  - ".join([str(model) for model in model_list])
                raise RuntimeError("Found {} models with the same name! Can't create tables. Name='{}'\n  - {}".format(len(model_list), model_name, models_str))

    def __write(self, build_filepath, payload):
        target_path = os.path.join(self.project['target-path'], build_filepath)

        if not os.path.exists(os.path.dirname(target_path)):
            os.makedirs(os.path.dirname(target_path))

        with open(target_path, 'w') as f:
            f.write(payload)


    def __ref(self, linker, ctx, model, all_models):
        schema = ctx['env']['schema']

        source_model = tuple(model.fqn)
        linker.add_node(source_model, {"materialized": model.materialization})

        def do_ref(*args):
            if len(args) == 1:
                other_model_name = self.create_template.model_name(args[0])
                other_model = find_model_by_name(all_models, other_model_name)
            elif len(args) == 2:
                other_model_package, other_model_name = args
                other_model_name = self.create_template.model_name(other_model_name)
                other_model = find_model_by_name(all_models, other_model_name, package_namespace=other_model_package)

            other_model_fqn = tuple(other_model.fqn[:-1] + [other_model_name])
            if not other_model.is_enabled:
                src_fqn = ".".join(source_model)
                ref_fqn = ".".join(other_model_fqn)
                raise RuntimeError("Model '{}' depends on model '{}' which is disabled in the project config".format(src_fqn, ref_fqn))

            linker.dependency(source_model, other_model_fqn)

            if other_model.is_ephemeral:
                linker.inject_cte(model, other_model)
                return other_model.cte_name
            else:
                return '"{}"."{}"'.format(schema, other_model_name)

        def wrapped_do_ref(*args):
            try:
                return do_ref(*args)
            except RuntimeError as e:
                print("Compiler error in {}".format(model.filepath))
                print("Enabled models:")
                for m in all_models:
                    print(" - {}".format(".".join(m.fqn)))
                raise e

        return wrapped_do_ref

    def compile_model(self, linker, model, models):
        jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=model.root_dir))

        if not model.is_enabled:
            return None

        template = jinja.get_template(model.rel_filepath)

        context = self.project.context()
        context['ref'] = self.__ref(linker, context, model, models)

        rendered = template.render(context)
        return rendered

    def __write_graph_file(self, linker):
        filename = 'graph-{}.yml'.format(self.create_template.label)
        graph_path = os.path.join(self.project['target-path'], filename)
        linker.write_graph(graph_path)

    def combine_query_with_ctes(self, model, query, ctes, compiled_models):
        parsed_stmts = sqlparse.parse(query)
        if len(parsed_stmts) != 1:
            raise RuntimeError("unexpectedly parsed {} queries from model {}".format(len(parsed_stmts), model))
        parsed = parsed_stmts[0]

        # TODO : i think there's a function to get this w/o iterating?
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
            # stmt exists, add a comma (which will come after our injected CTE(s) )
            trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ',')
            parsed.insert_after(with_stmt, trailing_comma)

        cte_mapping = [(model.cte_name, compiled_models[model]) for model in ctes]
        cte_stmts = [" {} as ( {} )".format(name, contents) for (name, contents) in cte_mapping]
        cte_text = ", ".join(cte_stmts)
        parsed.insert_after(with_stmt, cte_text)

        return sqlparse.format(str(parsed), keyword_case='lower', reindent=True)

    def add_cte_to_rendered_query(self, linker, primary_model, compiled_models):
        # this could be a set, but we're interested in maintaining the invocation order
        # psql CTEs need to be declared in-order and have to reference "upwards" in the CTE list

        required_ctes = []
        def add_ctes_recursive(model):
            if model not in linker.cte_map:
                return

            new_ctes = []
            for other_model in linker.cte_map[model]:
                if other_model not in required_ctes:
                    new_ctes.append(other_model)

            required_ctes.extend(new_ctes)
            for model in new_ctes: 
                add_ctes_recursive(model)
        add_ctes_recursive(primary_model)

        query = compiled_models[primary_model]
        if len(required_ctes) == 0:
            return query
        else:
            compiled_query = self.combine_query_with_ctes(primary_model, query, required_ctes, compiled_models)
            return compiled_query

    def compile(self):
        all_models = self.model_sources(this_project=self.project)

        for project in self.dependency_projects():
            all_models.extend(self.model_sources(this_project=self.project, own_project=project))

        models = [model for model in all_models if model.is_enabled]

        self.validate_models_unique(models)

        model_linker = Linker()
        compiled_models = {}
        written_models = 0
        for model in models:
            query = self.compile_model(model_linker, model, models)
            compiled_models[model] = query

        for model, query in compiled_models.items():
            injected_stmt = self.add_cte_to_rendered_query(model_linker, model, compiled_models)
            wrapped_stmt = model.compile(injected_stmt, self.project, self.create_template)

            build_path = model.build_path(self.create_template)

            if wrapped_stmt and not model.is_ephemeral:
                written_models += 1
                self.__write(build_path, wrapped_stmt)

        self.__write_graph_file(model_linker)

        # don't compile analyses in test mode!
        compiled_analyses = []
        if self.create_template.label != 'test':
            analysis_linker = Linker()
            analyses = self.analysis_sources(self.project)
            for analysis in analyses:
                compiled = self.compile_model(analysis_linker, analysis, models)
                if compiled:
                    compiled_analyses.append(compiled)

        return written_models, len(compiled_analyses)
