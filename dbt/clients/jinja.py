import dbt.compat
import dbt.exceptions

import jinja2
import jinja2.sandbox
import jinja2.nodes
import jinja2.ext

from dbt.node_types import NodeType


class MaterializationExtension(jinja2.ext.Extension):
    tags = set(['materialization'])

    def get_args(self):
        args = [
            'materialization',
            'model',
            'schema',
            'dist',
            'sort',
            'pre_hooks',
            'post_hooks',
            'sql',
            'flags',
            'adapter',
        ]

        return [jinja2.nodes.Name(arg, 'param') for arg in args]

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        materialization_name = parser.parse_assign_target(name_only=True).name

        node.name = "dbt__create_{}".format(materialization_name)
        node.args = self.get_args()
        node.defaults = []
        node.body = parser.parse_statements(('name:endmaterialization',),
                                            drop_needle=True)

        return node


def create_statement_extension(node, ctx, execute):

    class SQLStatementExtension(jinja2.ext.Extension):
        tags = set(['statement'])

        def parse(self, parser):
            lineno = next(parser.stream).lineno

            body = parser.parse_statements(['name:endstatement'],
                                           drop_needle=True)

            return jinja2.nodes.CallBlock(
                self.call_method('_run_statement',
                                 [jinja2.nodes.Const(execute)]),
                [], [], body).set_lineno(lineno)

        def _run_statement(self, execute, caller):
            body = caller()

            if execute:
                ctx['adapter'].add_query(ctx['profile'], body, node['name'])

            return body

    return SQLStatementExtension


def create_macro_validation_extension(node):

    class MacroContextCatcherExtension(jinja2.ext.Extension):
        DisallowedFuncs = ('ref', 'var')

        def onError(self, token):
            error = "The context variable '{}' is not allowed in macros." \
                    .format(token.value)
            dbt.exceptions.raise_compiler_error(node, error)

        def filter_stream(self, stream):
            while not stream.eos:
                token = next(stream)
                held = [token]

                if token.test('name') and token.value in self.DisallowedFuncs:
                    next_token = next(stream)
                    held.append(next_token)
                    if next_token.test('lparen'):
                        self.onError(token)

                for token in held:
                    yield token

    return MacroContextCatcherExtension


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


def get_template(string, ctx, node=None, capture_macros=False,
                 validate_macro=False, execute_statements=False):
    try:
        args = {
            'extensions': []
        }

        if capture_macros:
            args['undefined'] = create_macro_capture_env(node)

        if validate_macro:
            args['extensions'].append(create_macro_validation_extension(node))

        args['extensions'].append(MaterializationExtension)
        args['extensions'].append(
            create_statement_extension(node, ctx, execute_statements))

        env = jinja2.sandbox.SandboxedEnvironment(**args)

        return env.from_string(dbt.compat.to_string(string), globals=ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))


def render_template(template, ctx, node=None):
    try:
        return template.render(ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))


def get_rendered(string, ctx, node=None,
                 capture_macros=False,
                 execute_statements=False):
    template = get_template(string, ctx, node,
                            capture_macros=capture_macros,
                            execute_statements=execute_statements)
    return render_template(template, ctx, node)


def undefined_error(msg):
    raise jinja2.exceptions.UndefinedError(msg)
