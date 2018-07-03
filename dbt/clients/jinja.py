import jinja2
import jinja2._compat
import jinja2.ext
import jinja2.nodes
import jinja2.parser
import jinja2.sandbox

import dbt.compat
import dbt.exceptions

from dbt.node_types import NodeType
from dbt.utils import AttrDict

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class MacroFuzzParser(jinja2.parser.Parser):
    def parse_macro(self):
        node = jinja2.nodes.Macro(lineno=next(self.stream).lineno)

        # modified to fuzz macros defined in the same file. this way
        # dbt can understand the stack of macros being called.
        #  - @cmcarthur
        node.name = dbt.utils.get_dbt_macro_name(
            self.parse_assign_target(name_only=True).name)

        self.parse_signature(node)
        node.body = self.parse_statements(('name:endmacro',),
                                          drop_needle=True)
        return node


class MacroFuzzEnvironment(jinja2.sandbox.SandboxedEnvironment):
    def _parse(self, source, name, filename):
        return MacroFuzzParser(
            self, source, name,
            jinja2._compat.encode_filename(filename)
        ).parse()


def macro_generator(template, node):
    def apply_context(context):
        def call(*args, **kwargs):
            name = node.get('name')
            module = template.make_module(
                context, False, context)

            if node['resource_type'] == NodeType.Operation:
                macro = module.__dict__[dbt.utils.get_dbt_operation_name(name)]
            else:
                macro = module.__dict__[dbt.utils.get_dbt_macro_name(name)]
            module.__dict__.update(context)

            try:
                return macro(*args, **kwargs)
            except dbt.exceptions.MacroReturn as e:
                return e.value
            except (TypeError,
                    jinja2.exceptions.TemplateRuntimeError) as e:
                dbt.exceptions.raise_compiler_error(
                    str(e),
                    node)
            except dbt.exceptions.CompilationException as e:
                e.stack.append(node)
                raise e

        return call
    return apply_context


class MaterializationExtension(jinja2.ext.Extension):
    tags = ['materialization']

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


class OperationExtension(jinja2.ext.Extension):
    tags = ['operation']

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        operation_name = \
            parser.parse_assign_target(name_only=True).name

        node.args = []
        node.defaults = []

        while parser.stream.skip_if('comma'):
            target = parser.parse_assign_target(name_only=True)

        node.name = dbt.utils.get_operation_macro_name(operation_name)

        node.body = parser.parse_statements(('name:endoperation',),
                                            drop_needle=True)

        return node


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
        args['extensions'].append(OperationExtension)

        env = MacroFuzzEnvironment(**args)

        return env.from_string(dbt.compat.to_string(string), globals=ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        e.translated = False
        dbt.exceptions.raise_compiler_error(str(e), node)


def render_template(template, ctx, node=None):
    try:
        return template.render(ctx)

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        e.translated = False
        dbt.exceptions.raise_compiler_error(str(e), node)


def get_rendered(string, ctx, node=None,
                 capture_macros=False):
    template = get_template(string, ctx, node,
                            capture_macros=capture_macros)

    return render_template(template, ctx, node)


def undefined_error(msg):
    raise jinja2.exceptions.UndefinedError(msg)
