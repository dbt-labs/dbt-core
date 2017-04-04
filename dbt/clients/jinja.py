import dbt.compat
import dbt.exceptions

import jinja2
import jinja2.sandbox
import jinja2.nodes
import jinja2.ext

from dbt.utils import NodeType


def create_macro_validation_extension(node):

    class MacroContextCatcherExtension(jinja2.ext.Extension):
        DisallowedFuncs = ('ref', 'var')
        DisallowedVars = ('this', 'target')

        def __init__(self, *args, **kwargs):
            self.macro_arguments = set()
            super(MacroContextCatcherExtension, self).__init__(*args, **kwargs)

        def onError(self, token):
            error = "The context variable '{}' is not allowed in macros." \
                    .format(token.value)
            dbt.exceptions.raise_compiler_error(node, error)

        def read_until(self, stream, token_type):
            held = []
            while not stream.eos:
                token = next(stream)
                held.append(token)
                if token.test(token_type):
                    break
            return held

        def parse_args(self, tokens):
            expected_prefix_types = [
                'name',
                'name',
                'lparen'
            ]

            expected_suffix_types = [
                'rparen',
                'block_end'
            ]

            min_len = len(expected_prefix_types) + len(expected_suffix_types)
            if len(tokens) <= min_len:
                return None

            prefix_types = [t.type for t in tokens[:3]]
            suffix_types = [t.type for t in tokens[-2:]]

            # ensure the macro definition is valid. otherwise, garbage in, garbage out
            if tokens[0].value != 'macro' or \
               prefix_types != expected_prefix_types or \
               suffix_types != expected_suffix_types:
                return None

            args = [token.value for token in tokens[3:-2] if not token.test('comma')]
            return args

        def filter_stream(self, stream):
            while not stream.eos:
                token = next(stream)
                held = [token]

                if token.test('block_begin'):
                    tokens = self.read_until(stream, 'block_end')
                    args = self.parse_args(tokens)
                    if args is not None:
                        self.macro_arguments.update(args)
                    held.extend(tokens)

                elif token.test('name') and token.value in self.DisallowedFuncs:
                    next_token = next(stream)
                    held.append(next_token)
                    if next_token.test('lparen'):
                        self.onError(token)

                elif token.test('name') and token.value in self.DisallowedVars:
                    # allow this var if it is passed in as an arg!
                    if token.value not in self.macro_arguments:
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

    return jinja2.sandbox.SandboxedEnvironment(
        undefined=ParserMacroCapture)


env = jinja2.sandbox.SandboxedEnvironment()


def get_template(string, ctx, node=None, capture_macros=False,
                 validate_macro=False):
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
