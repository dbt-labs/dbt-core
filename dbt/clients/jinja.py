import dbt.compat
import dbt.exceptions

import jinja2
import jinja2.sandbox
import jinja2.nodes
import jinja2.ext
import jinja2.lexer

from dbt.utils import NodeType


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


def create_ref_capture_env(ref_capture_node, do_ref_capture):

    class RefCaptureExtension(jinja2.ext.Extension):
        node = ref_capture_node
        do_ref = do_ref_capture

        def __init__(self, environment):
            super(RefCaptureExtension, self).__init__(environment)

        @classmethod
        def extract_referenced_name(cls, tokens):
            for _ in range(len(tokens)):
                token = tokens.pop(0)
                if token.test('name') and token.value == 'ref':
                    break

            ref_args = []
            for token in tokens:
                if token.test('lparen') or token.test('comma'):
                    continue
                elif token.test('rparen'):
                    break
                elif token.test('string'):
                    ref_args.append(token.value)

            return ref_args

        @classmethod
        def filter_stream(cls, stream):
            while not stream.eos:
                token = next(stream)
                if token.test("variable_begin") or token.test('block_begin'):
                    var_expr = []
                    while not token.test("variable_end") and not token.test('block_begin'):
                        var_expr.append(token)
                        token = next(stream)
                    variable_end = token

                    is_ref_expr = any(t.value == 'ref' for t in var_expr)
                    if is_ref_expr:
                        referenced_model_tokens = cls.extract_referenced_name(var_expr[:])
                        cls.do_ref(*referenced_model_tokens)

                    var_expr.append(variable_end)
                    for token in var_expr:
                        yield token
                else:
                    yield token

    return RefCaptureExtension


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

        if 'ref' in ctx:
            local_env.add_extension(create_ref_capture_env(node, ctx['ref']))

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
