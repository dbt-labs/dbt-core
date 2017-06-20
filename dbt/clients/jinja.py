import dbt.compat
import dbt.exceptions

import jinja2
import jinja2.sandbox
import jinja2.nodes
import jinja2.ext

from dbt.utils import NodeType


class MaterializationExtension(jinja2.ext.Extension):
    tags = set(['materialization'])

    def parse_signature(self, node):
        node.args = []
        node.defaults = []

        # TODO add the exhaustive list of automatically passed
        # materialization macro args here
        return

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        materialization_name = parser.parse_assign_target(name_only=True).name
        node.name = "dbt__{}".format(materialization_name)
        self.parse_signature(node)
        node.body = parser.parse_statements(('name:endmaterialization',),
                                            drop_needle=True)

        print(node)
        return node


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

    return jinja2.sandbox.SandboxedEnvironment(
        extensions=[MacroContextCatcherExtension])


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

    return jinja2.sandbox.SandboxedEnvironment(
        undefined=ParserMacroCapture)


env = jinja2.sandbox.SandboxedEnvironment(
    extensions=[MaterializationExtension])


def get_template(string, ctx, node=None, capture_macros=False,
                 validate_macro=False):
    global env

    try:
        local_env = env

        if capture_macros:
            local_env = create_macro_capture_env(node)

        elif validate_macro:
            local_env = create_macro_validation_extension(node)

        return local_env.from_string(dbt.compat.to_string(string), globals=ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))


def render_template(template, ctx, node=None):
    try:
        return template.render(ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        dbt.exceptions.raise_compiler_error(node, str(e))


def get_rendered(string, ctx, node=None, capture_macros=False):
    template = get_template(string, ctx, node, capture_macros)
    return render_template(template, ctx, node)
