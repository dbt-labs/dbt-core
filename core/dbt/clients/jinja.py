import codecs
import linecache
import os

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

    def _compile(self, source, filename):
        """Override jinja's compilation to stash the rendered source inside
        the python linecache for debugging.
        """
        if filename == '<template>':
            # make a better filename
            filename = 'dbt-{}'.format(
                codecs.encode(os.urandom(12), 'hex').decode('ascii')
            )
            # encode, though I don't think this matters
            filename = jinja2._compat.encode_filename(filename)
            # put ourselves in the cache
            linecache.cache[filename] = (
                len(source),
                None,
                [line+'\n' for line in source.splitlines()],
                filename
            )

        return super(MacroFuzzEnvironment, self)._compile(source, filename)


class TemplateCache(object):

    def __init__(self):
        self.file_cache = {}

    def get_node_template(self, node):
        key = (node['package_name'], node['original_file_path'])

        if key in self.file_cache:
            return self.file_cache[key]

        template = get_template(
            string=node.get('raw_sql'),
            ctx={},
            node=node
        )
        self.file_cache[key] = template

        return template

    def clear(self):
        self.file_cache.clear()


template_cache = TemplateCache()


def macro_generator(node):
    def apply_context(context):
        def call(*args, **kwargs):
            name = node.get('name')
            template = template_cache.get_node_template(node)
            module = template.make_module(context, False, context)

            macro = module.__dict__[dbt.utils.get_dbt_macro_name(name)]
            module.__dict__.update(context)

            try:
                return macro(*args, **kwargs)
            except dbt.exceptions.MacroReturn as e:
                return e.value
            except (TypeError, jinja2.exceptions.TemplateRuntimeError) as e:
                dbt.exceptions.raise_compiler_error(str(e), node)
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


class DocumentationExtension(jinja2.ext.Extension):
    tags = ['docs']

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        docs_name = parser.parse_assign_target(name_only=True).name

        node.args = []
        node.defaults = []
        node.name = dbt.utils.get_docs_macro_name(docs_name)
        node.body = parser.parse_statements(('name:enddocs',),
                                            drop_needle=True)
        return node


def _is_dunder_name(name):
    return name.startswith('__') and name.endswith('__')


def create_macro_capture_env(node):

    class ParserMacroCapture(jinja2.Undefined):
        """
        This class sets up the parser to capture macros.
        """
        def __init__(self, hint=None, obj=None, name=None, exc=None):
            super(ParserMacroCapture, self).__init__(hint=hint, name=name)
            self.node = node
            self.name = name
            self.package_name = node.get('package_name')
            # jinja uses these for safety, so we have to override them.
            # see https://github.com/pallets/jinja/blob/master/jinja2/sandbox.py#L332-L339 # noqa
            self.unsafe_callable = False
            self.alters_data = False

        def __deepcopy__(self, memo):
            path = os.path.join(self.node.get('root_path'),
                                self.node.get('original_file_path'))

            logger.debug(
                'dbt encountered an undefined variable, "{}" in node {}.{} '
                '(source path: {})'
                .format(self.name, self.node.get('package_name'),
                        self.node.get('name'), path))

            # match jinja's message
            dbt.exceptions.raise_compiler_error(
                "{!r} is undefined".format(self.name),
                node=self.node
            )

        def __getitem__(self, name):
            # Propagate the undefined value if a caller accesses this as if it
            # were a dictionary
            return self

        def __getattr__(self, name):
            if name == 'name' or _is_dunder_name(name):
                raise AttributeError(
                    "'{}' object has no attribute '{}'"
                    .format(type(self).__name__, name)
                )

            self.package_name = self.name
            self.name = name

            return self

        def __call__(self, *args, **kwargs):
            return True

    return ParserMacroCapture


def get_environment(node=None, capture_macros=False):
    args = {
        'extensions': ['jinja2.ext.do']
    }

    if capture_macros:
        args['undefined'] = create_macro_capture_env(node)

    args['extensions'].append(MaterializationExtension)
    args['extensions'].append(DocumentationExtension)

    return MacroFuzzEnvironment(**args)


def parse(string):
    try:
        return get_environment().parse(dbt.compat.to_string(string))

    except (jinja2.exceptions.TemplateSyntaxError,
            jinja2.exceptions.UndefinedError) as e:
        e.translated = False
        dbt.exceptions.raise_compiler_error(str(e))


def get_template(string, ctx, node=None, capture_macros=False):
    try:
        env = get_environment(node, capture_macros)

        template_source = dbt.compat.to_string(string)
        return env.from_string(template_source, globals=ctx)

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
