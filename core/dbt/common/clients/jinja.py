import codecs
import linecache
import os
import tempfile
from ast import literal_eval
from contextlib import contextmanager
from itertools import chain, islice
from typing import List, Union, Set, Optional, Dict, Any, Iterator, Type, Callable
from typing_extensions import Protocol

import jinja2
import jinja2.ext
import jinja2.nativetypes  # type: ignore
import jinja2.nodes
import jinja2.parser
import jinja2.sandbox

from dbt.common.utils import (
    get_dbt_macro_name,
    get_docs_macro_name,
    get_materialization_macro_name,
    get_test_macro_name,
)
from dbt.common.clients._jinja_blocks import BlockIterator, BlockData, BlockTag

from dbt.common.exceptions import (
    CompilationError,
    DbtInternalError,
    CaughtMacroErrorWithNodeError,
    MaterializationArgError,
    JinjaRenderingError,
    UndefinedCompilationError,
)
from dbt.common.exceptions.macros import MacroReturn, UndefinedMacroError, CaughtMacroError


SUPPORTED_LANG_ARG = jinja2.nodes.Name("supported_languages", "param")

# Global which can be set by dependents of dbt-common (e.g. core via flag parsing)
MACRO_DEBUGGING = False


def _linecache_inject(source, write):
    if write:
        # this is the only reliable way to accomplish this. Obviously, it's
        # really darn noisy and will fill your temporary directory
        tmp_file = tempfile.NamedTemporaryFile(
            prefix="dbt-macro-compiled-",
            suffix=".py",
            delete=False,
            mode="w+",
            encoding="utf-8",
        )
        tmp_file.write(source)
        filename = tmp_file.name
    else:
        # `codecs.encode` actually takes a `bytes` as the first argument if
        # the second argument is 'hex' - mypy does not know this.
        rnd = codecs.encode(os.urandom(12), "hex")  # type: ignore
        filename = rnd.decode("ascii")

    # put ourselves in the cache
    cache_entry = (len(source), None, [line + "\n" for line in source.splitlines()], filename)
    # linecache does in fact have an attribute `cache`, thanks
    linecache.cache[filename] = cache_entry  # type: ignore
    return filename


class MacroFuzzParser(jinja2.parser.Parser):
    def parse_macro(self):
        node = jinja2.nodes.Macro(lineno=next(self.stream).lineno)

        # modified to fuzz macros defined in the same file. this way
        # dbt can understand the stack of macros being called.
        #  - @cmcarthur
        node.name = get_dbt_macro_name(self.parse_assign_target(name_only=True).name)

        self.parse_signature(node)
        node.body = self.parse_statements(("name:endmacro",), drop_needle=True)
        return node


class MacroFuzzEnvironment(jinja2.sandbox.SandboxedEnvironment):
    def _parse(self, source, name, filename):
        return MacroFuzzParser(self, source, name, filename).parse()

    def _compile(self, source, filename):
        """Override jinja's compilation to stash the rendered source inside
        the python linecache for debugging when the appropriate environment
        variable is set.

        If the value is 'write', also write the files to disk.
        WARNING: This can write a ton of data if you aren't careful.
        """
        if filename == "<template>" and MACRO_DEBUGGING:
            write = MACRO_DEBUGGING == "write"
            filename = _linecache_inject(source, write)

        return super()._compile(source, filename)  # type: ignore


class NativeSandboxEnvironment(MacroFuzzEnvironment):
    code_generator_class = jinja2.nativetypes.NativeCodeGenerator


class TextMarker(str):
    """A special native-env marker that indicates a value is text and is
    not to be evaluated. Use this to prevent your numbery-strings from becoming
    numbers!
    """


class NativeMarker(str):
    """A special native-env marker that indicates the field should be passed to
    literal_eval.
    """


class BoolMarker(NativeMarker):
    pass


class NumberMarker(NativeMarker):
    pass


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def quoted_native_concat(nodes):
    """This is almost native_concat from the NativeTemplate, except in the
    special case of a single argument that is a quoted string and returns a
    string, the quotes are re-inserted.
    """
    head = list(islice(nodes, 2))

    if not head:
        return ""

    if len(head) == 1:
        raw = head[0]
        if isinstance(raw, TextMarker):
            return str(raw)
        elif not isinstance(raw, NativeMarker):
            # return non-strings as-is
            return raw
    else:
        # multiple nodes become a string.
        return "".join([str(v) for v in chain(head, nodes)])

    try:
        result = literal_eval(raw)
    except (ValueError, SyntaxError, MemoryError):
        result = raw
    if isinstance(raw, BoolMarker) and not isinstance(result, bool):
        raise JinjaRenderingError(f"Could not convert value '{raw!s}' into type 'bool'")
    if isinstance(raw, NumberMarker) and not _is_number(result):
        raise JinjaRenderingError(f"Could not convert value '{raw!s}' into type 'number'")

    return result


class NativeSandboxTemplate(jinja2.nativetypes.NativeTemplate):  # mypy: ignore
    environment_class = NativeSandboxEnvironment  # type: ignore

    def render(self, *args, **kwargs):
        """Render the template to produce a native Python type. If the
        result is a single node, its value is returned. Otherwise, the
        nodes are concatenated as strings. If the result can be parsed
        with :func:`ast.literal_eval`, the parsed value is returned.
        Otherwise, the string is returned.
        """
        vars = dict(*args, **kwargs)

        try:
            return quoted_native_concat(self.root_render_func(self.new_context(vars)))
        except Exception:
            return self.environment.handle_exception()


NativeSandboxEnvironment.template_class = NativeSandboxTemplate  # type: ignore


class TemplateCache:
    def __init__(self) -> None:
        self.file_cache: Dict[str, jinja2.Template] = {}

    def get_node_template(self, node) -> jinja2.Template:
        key = node.macro_sql

        if key in self.file_cache:
            return self.file_cache[key]

        template = get_template(
            string=node.macro_sql,
            ctx={},
            node=node,
        )

        self.file_cache[key] = template
        return template

    def clear(self):
        self.file_cache.clear()


template_cache = TemplateCache()


class BaseMacroGenerator:
    def __init__(self, context: Optional[Dict[str, Any]] = None) -> None:
        self.context: Optional[Dict[str, Any]] = context

    def get_template(self):
        raise NotImplementedError("get_template not implemented!")

    def get_name(self) -> str:
        raise NotImplementedError("get_name not implemented!")

    def get_macro(self):
        name = self.get_name()
        template = self.get_template()
        # make the module. previously we set both vars and local, but that's
        # redundant: They both end up in the same place
        # make_module is in jinja2.environment. It returns a TemplateModule
        module = template.make_module(vars=self.context, shared=False)
        macro = module.__dict__[get_dbt_macro_name(name)]
        module.__dict__.update(self.context)
        return macro

    @contextmanager
    def exception_handler(self) -> Iterator[None]:
        try:
            yield
        except (TypeError, jinja2.exceptions.TemplateRuntimeError) as e:
            raise CaughtMacroError(e)

    def call_macro(self, *args, **kwargs):
        # called from __call__ methods
        if self.context is None:
            raise DbtInternalError("Context is still None in call_macro!")
        assert self.context is not None

        macro = self.get_macro()

        with self.exception_handler():
            try:
                return macro(*args, **kwargs)
            except MacroReturn as e:
                return e.value


class MacroProtocol(Protocol):
    name: str
    macro_sql: str


class CallableMacroGenerator(BaseMacroGenerator):
    def __init__(
        self,
        macro: MacroProtocol,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(context)
        self.macro = macro

    def get_template(self):
        return template_cache.get_node_template(self.macro)

    def get_name(self) -> str:
        return self.macro.name

    @contextmanager
    def exception_handler(self) -> Iterator[None]:
        try:
            yield
        except (TypeError, jinja2.exceptions.TemplateRuntimeError) as e:
            raise CaughtMacroErrorWithNodeError(exc=e, node=self.macro)
        except CompilationError as e:
            e.stack.append(self.macro)
            raise e

    # this makes MacroGenerator objects callable like functions
    def __call__(self, *args, **kwargs):
        return self.call_macro(*args, **kwargs)


class MaterializationExtension(jinja2.ext.Extension):
    tags = ["materialization"]

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        materialization_name = parser.parse_assign_target(name_only=True).name

        adapter_name = "default"
        node.args = []
        node.defaults = []

        while parser.stream.skip_if("comma"):
            target = parser.parse_assign_target(name_only=True)

            if target.name == "default":
                pass

            elif target.name == "adapter":
                parser.stream.expect("assign")
                value = parser.parse_expression()
                adapter_name = value.value

            elif target.name == "supported_languages":
                target.set_ctx("param")
                node.args.append(target)
                parser.stream.expect("assign")
                languages = parser.parse_expression()
                node.defaults.append(languages)

            else:
                raise MaterializationArgError(materialization_name, target.name)

        if SUPPORTED_LANG_ARG not in node.args:
            node.args.append(SUPPORTED_LANG_ARG)
            node.defaults.append(jinja2.nodes.List([jinja2.nodes.Const("sql")]))

        node.name = get_materialization_macro_name(materialization_name, adapter_name)

        node.body = parser.parse_statements(("name:endmaterialization",), drop_needle=True)

        return node


class DocumentationExtension(jinja2.ext.Extension):
    tags = ["docs"]

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        docs_name = parser.parse_assign_target(name_only=True).name

        node.args = []
        node.defaults = []
        node.name = get_docs_macro_name(docs_name)
        node.body = parser.parse_statements(("name:enddocs",), drop_needle=True)
        return node


class TestExtension(jinja2.ext.Extension):
    tags = ["test"]

    def parse(self, parser):
        node = jinja2.nodes.Macro(lineno=next(parser.stream).lineno)
        test_name = parser.parse_assign_target(name_only=True).name

        parser.parse_signature(node)
        node.name = get_test_macro_name(test_name)
        node.body = parser.parse_statements(("name:endtest",), drop_needle=True)
        return node


def _is_dunder_name(name):
    return name.startswith("__") and name.endswith("__")


def create_undefined(node=None):
    class Undefined(jinja2.Undefined):
        def __init__(self, hint=None, obj=None, name=None, exc=None):
            super().__init__(hint=hint, name=name)
            self.node = node
            self.name = name
            self.hint = hint
            # jinja uses these for safety, so we have to override them.
            # see https://github.com/pallets/jinja/blob/master/jinja2/sandbox.py#L332-L339 # noqa
            self.unsafe_callable = False
            self.alters_data = False

        def __getitem__(self, name):
            # Propagate the undefined value if a caller accesses this as if it
            # were a dictionary
            return self

        def __getattr__(self, name):
            if name == "name" or _is_dunder_name(name):
                raise AttributeError(
                    "'{}' object has no attribute '{}'".format(type(self).__name__, name)
                )

            self.name = name

            return self.__class__(hint=self.hint, name=self.name)

        def __call__(self, *args, **kwargs):
            return self

        def __reduce__(self):
            raise UndefinedCompilationError(name=self.name, node=node)

    return Undefined


NATIVE_FILTERS: Dict[str, Callable[[Any], Any]] = {
    "as_text": TextMarker,
    "as_bool": BoolMarker,
    "as_native": NativeMarker,
    "as_number": NumberMarker,
}


TEXT_FILTERS: Dict[str, Callable[[Any], Any]] = {
    "as_text": lambda x: x,
    "as_bool": lambda x: x,
    "as_native": lambda x: x,
    "as_number": lambda x: x,
}


def get_environment(
    node=None,
    capture_macros: bool = False,
    native: bool = False,
) -> jinja2.Environment:
    args: Dict[str, List[Union[str, Type[jinja2.ext.Extension]]]] = {
        "extensions": ["jinja2.ext.do", "jinja2.ext.loopcontrols"]
    }

    if capture_macros:
        args["undefined"] = create_undefined(node)

    args["extensions"].append(MaterializationExtension)
    args["extensions"].append(DocumentationExtension)
    args["extensions"].append(TestExtension)

    env_cls: Type[jinja2.Environment]
    text_filter: Type
    if native:
        env_cls = NativeSandboxEnvironment
        filters = NATIVE_FILTERS
    else:
        env_cls = MacroFuzzEnvironment
        filters = TEXT_FILTERS

    env = env_cls(**args)
    env.filters.update(filters)

    return env


@contextmanager
def catch_jinja(node=None) -> Iterator[None]:
    try:
        yield
    except jinja2.exceptions.TemplateSyntaxError as e:
        e.translated = False
        raise CompilationError(str(e), node) from e
    except jinja2.exceptions.UndefinedError as e:
        raise UndefinedMacroError(str(e), node) from e
    except CompilationError as exc:
        exc.add_node(node)
        raise


def parse(string):
    with catch_jinja():
        return get_environment().parse(str(string))


def get_template(
    string: str,
    ctx: Dict[str, Any],
    node=None,
    capture_macros: bool = False,
    native: bool = False,
):
    with catch_jinja(node):
        env = get_environment(node, capture_macros, native=native)

        template_source = str(string)
        return env.from_string(template_source, globals=ctx)


def render_template(template, ctx: Dict[str, Any], node=None) -> str:
    with catch_jinja(node):
        return template.render(ctx)


def extract_toplevel_blocks(
    data: str,
    allowed_blocks: Optional[Set[str]] = None,
    collect_raw_data: bool = True,
) -> List[Union[BlockData, BlockTag]]:
    """Extract the top-level blocks with matching block types from a jinja
    file, with some special handling for block nesting.

    :param data: The data to extract blocks from.
    :param allowed_blocks: The names of the blocks to extract from the file.
        They may not be nested within if/for blocks. If None, use the default
        values.
    :param collect_raw_data: If set, raw data between matched blocks will also
        be part of the results, as `BlockData` objects. They have a
        `block_type_name` field of `'__dbt_data'` and will never have a
        `block_name`.
    :return: A list of `BlockTag`s matching the allowed block types and (if
        `collect_raw_data` is `True`) `BlockData` objects.
    """
    return BlockIterator(data).lex_for_blocks(
        allowed_blocks=allowed_blocks, collect_raw_data=collect_raw_data
    )
