import jinja2
import jinja2.sandbox
import jinja2.nodes
import jinja2.ext

import dbt.compat
import dbt.exceptions

from dbt.node_types import NodeType
from dbt.utils import AttrDict

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


def macro_generator(template, name):
    def apply_context(context):
        def call(*args, **kwargs):
            module = template.make_module(
                context, False, {})
            macro = module.__dict__[name]
            module.__dict__ = context

            return macro(*args, **kwargs)

        return call
    return apply_context


class MaterializationExtension(jinja2.ext.Extension):
    tags = set(['materialization'])

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        materialization_name = \
            parser.parse_assign_target(name_only=True).name

        adapter_name = 'default'
        node.args = []
        node.defaults = []

        while parser.stream.skip_if('comma'):
            target = parser.parse_assign_target(name_only=True)

            if target.name == 'default':
                pass

            elif target.name == 'adapter':
                parser.stream.expect('assign')
                value = parser.parse_expression()
                adapter_name = value.value

            else:
                dbt.exceptions.invalid_materialization_argument(
                    materialization_name, target.name)

        node.name = dbt.utils.get_materialization_macro_name(
            materialization_name, adapter_name)

        node.body = parser.parse_statements(('name:endmaterialization',),
                                            drop_needle=True)

        return node


class SQLStatementExtension(jinja2.ext.Extension):
    tags = set(['statement'])

    def try_render(self, model, caller, context):
        try:
            pre_rendered_body = caller()
        except BaseException as e:
            dbt.exceptions.raise_compiler_error(model, str(e))

        # we have to re-render the body to handle cases where jinja
        # is passed in as an argument, i.e. where an incremental
        # `sql_where` includes {{this}}
        return dbt.clients.jinja.get_rendered(
            pre_rendered_body,
            context,
            model)

    def result_from_cursor(self, cursor):
        data = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            raw_results = cursor.fetchall()
            data = [dict(zip(column_names, row))
                    for row in raw_results]

        return data

    def capture_result(self, store_result_as, store_result, adapter, cursor):
        if store_result and store_result_as:
            status = adapter.get_status(cursor)

            data = self.result_from_cursor(cursor)

            setattr(self.environment, store_result_as,
                    AttrDict({'status': status,
                              'data': data}))

            store_result(store_result_as, status=status, data=data)

    def _execute_body(self, store_result_as, store_result,
                      execute, adapter, context, model, caller):
        body = self.try_render(model, caller, context)

        if not execute:
            return body

        connection, cursor = adapter.add_query(body)

        self.capture_result(store_result_as, store_result, adapter, cursor)

        return body

    def parse(self, parser):
        lineno = next(parser.stream).lineno

        token = parser.stream.next_if('name')

        store_result_as = None

        if token:
            store_result_as = token.value

        body = parser.parse_statements(
            ['name:endstatement'],
            drop_needle=True)

        callblock = jinja2.nodes.CallBlock(
            self.call_method(
                '_execute_body',
                [jinja2.nodes.Const(store_result_as),
                 jinja2.nodes.Name('store_result', 'load'),
                 jinja2.nodes.Name('execute', 'load'),
                 jinja2.nodes.Name('adapter', 'load'),
                 jinja2.nodes.ContextReference(),
                 jinja2.nodes.Name('model', 'load')]),
            [], [], body).set_lineno(lineno)

        return callblock


def create_macro_capture_env(node):

    class ParserMacroCapture(jinja2.Undefined):
        """
        This class sets up the parser to capture macros.
        """
        def __init__(self, hint=None, obj=None, name=None,
                     exc=None):
            super(jinja2.Undefined, self).__init__()

            self.node = node
            self.name = name
            self.package_name = node.get('package_name')

        def __getattr__(self, name):

            # jinja uses these for safety, so we have to override them.
            # see https://github.com/pallets/jinja/blob/master/jinja2/sandbox.py#L332-L339 # noqa
            if name in ['unsafe_callable', 'alters_data']:
                return False

            self.package_name = self.name
            self.name = name

            return self

        def __call__(self, *args, **kwargs):
            path = '{}.{}.{}'.format(NodeType.Macro,
                                     self.package_name,
                                     self.name)

            if path not in self.node['depends_on']['macros']:
                self.node['depends_on']['macros'].append(path)

            return True

    return ParserMacroCapture


def get_template(string, ctx, node=None, capture_macros=False):
    try:
        args = {
            'extensions': []
        }

        if capture_macros:
            args['undefined'] = create_macro_capture_env(node)

        args['extensions'].append(MaterializationExtension)
        args['extensions'].append(SQLStatementExtension)

        env = jinja2.sandbox.SandboxedEnvironment(**args)

        return env.from_string(dbt.compat.to_string(string), globals=ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        e.translated = False
        dbt.exceptions.raise_compiler_error(node, str(e))


def render_template(template, ctx, node=None):
    try:
        return template.render(ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        e.translated = False
        dbt.exceptions.raise_compiler_error(node, str(e))


def get_rendered(string, ctx, node=None,
                 capture_macros=False):
    template = get_template(string, ctx, node,
                            capture_macros=capture_macros)

    return render_template(template, ctx, node)


def undefined_error(msg):
    raise jinja2.exceptions.UndefinedError(msg)
