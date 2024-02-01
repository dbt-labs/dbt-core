import re
import threading
from contextlib import contextmanager
from typing import List, Union, Optional, Dict, Any, NoReturn, Tuple

import jinja2
import jinja2.ext
import jinja2.nativetypes  # type: ignore
import jinja2.nodes
import jinja2.parser
import jinja2.sandbox

from dbt_common.clients.jinja import (
    render_template,
    get_template,
    CallableMacroGenerator,
    MacroProtocol,
)
from dbt_common.utils import deep_map_render
from dbt.contracts.graph.nodes import GenericTestNode

from dbt.exceptions import (
    DbtInternalError,
    MaterializtionMacroNotUsedError,
    NoSupportedLanguagesFoundError,
)
from dbt.node_types import ModelLanguage


SUPPORTED_LANG_ARG = jinja2.nodes.Name("supported_languages", "param")


class MacroStack(threading.local):
    def __init__(self):
        super().__init__()
        self.call_stack = []

    @property
    def depth(self) -> int:
        return len(self.call_stack)

    def push(self, name):
        self.call_stack.append(name)

    def pop(self, name):
        got = self.call_stack.pop()
        if got != name:
            raise DbtInternalError(f"popped {got}, expected {name}")


class MacroGenerator(CallableMacroGenerator):
    def __init__(
        self,
        macro: MacroProtocol,
        context: Optional[Dict[str, Any]] = None,
        node: Optional[Any] = None,
        stack: Optional[MacroStack] = None,
    ) -> None:
        super().__init__(macro, context)
        self.node = node
        self.stack = stack

    # This adds the macro's unique id to the node's 'depends_on'
    @contextmanager
    def track_call(self):
        # This is only called from __call__
        if self.stack is None:
            yield
        else:
            unique_id = self.macro.unique_id
            depth = self.stack.depth
            # only mark depth=0 as a dependency, when creating this dependency we don't pass in stack
            if depth == 0 and self.node:
                self.node.depends_on.add_macro(unique_id)
            self.stack.push(unique_id)
            try:
                yield
            finally:
                self.stack.pop(unique_id)

    # this makes MacroGenerator objects callable like functions
    def __call__(self, *args, **kwargs):
        with self.track_call():
            return self.call_macro(*args, **kwargs)


class UnitTestMacroGenerator(MacroGenerator):
    # this makes UnitTestMacroGenerator objects callable like functions
    def __init__(
        self,
        macro_generator: MacroGenerator,
        call_return_value: Any,
    ) -> None:
        super().__init__(
            macro_generator.macro,
            macro_generator.context,
            macro_generator.node,
            macro_generator.stack,
        )
        self.call_return_value = call_return_value

    def __call__(self, *args, **kwargs):
        with self.track_call():
            return self.call_return_value


# performance note: Local benmcharking (so take it with a big grain of salt!)
# on this indicates that it is is on average slightly slower than
# checking two separate patterns, but the standard deviation is smaller with
# one pattern. The time difference between the two was ~2 std deviations, which
# is small enough that I've just chosen the more readable option.
_HAS_RENDER_CHARS_PAT = re.compile(r"({[{%#]|[#}%]})")

_render_cache: Dict[str, Any] = dict()


def get_rendered(
    string: str,
    ctx: Dict[str, Any],
    node=None,
    capture_macros: bool = False,
    native: bool = False,
) -> Any:
    # performance optimization: if there are no jinja control characters in the
    # string, we can just return the input. Fall back to jinja if the type is
    # not a string or if native rendering is enabled (so '1' -> 1, etc...)
    # If this is desirable in the native env as well, we could handle the
    # native=True case by passing the input string to ast.literal_eval, like
    # the native renderer does.
    has_render_chars = not isinstance(string, str) or _HAS_RENDER_CHARS_PAT.search(string)

    if not has_render_chars:
        if not native:
            return string
        elif string in _render_cache:
            return _render_cache[string]

    template = get_template(
        string,
        ctx,
        node,
        capture_macros=capture_macros,
        native=native,
    )

    rendered = render_template(template, ctx, node)

    if not has_render_chars and native:
        _render_cache[string] = rendered

    return rendered


def undefined_error(msg) -> NoReturn:
    raise jinja2.exceptions.UndefinedError(msg)


GENERIC_TEST_KWARGS_NAME = "_dbt_generic_test_kwargs"


def add_rendered_test_kwargs(
    context: Dict[str, Any],
    node: GenericTestNode,
    capture_macros: bool = False,
) -> None:
    """Render each of the test kwargs in the given context using the native
    renderer, then insert that value into the given context as the special test
    keyword arguments member.
    """
    looks_like_func = r"^\s*(env_var|ref|var|source|doc)\s*\(.+\)\s*$"

    def _convert_function(value: Any, keypath: Tuple[Union[str, int], ...]) -> Any:
        if isinstance(value, str):
            if keypath == ("column_name",):
                # special case: Don't render column names as native, make them
                # be strings
                return value

            if re.match(looks_like_func, value) is not None:
                # curly braces to make rendering happy
                value = f"{{{{ {value} }}}}"

            value = get_rendered(value, context, node, capture_macros=capture_macros, native=True)

        return value

    # The test_metadata.kwargs come from the test builder, and were set
    # when the test node was created in _parse_generic_test.
    kwargs = deep_map_render(_convert_function, node.test_metadata.kwargs)
    context[GENERIC_TEST_KWARGS_NAME] = kwargs


def get_supported_languages(node: jinja2.nodes.Macro) -> List[ModelLanguage]:
    if "materialization" not in node.name:
        raise MaterializtionMacroNotUsedError(node=node)

    no_kwargs = not node.defaults
    no_langs_found = SUPPORTED_LANG_ARG not in node.args

    if no_kwargs or no_langs_found:
        raise NoSupportedLanguagesFoundError(node=node)

    lang_idx = node.args.index(SUPPORTED_LANG_ARG)
    # indexing defaults from the end
    # since supported_languages is a kwarg, and kwargs are at always after args
    return [
        ModelLanguage[item.value] for item in node.defaults[-(len(node.args) - lang_idx)].items
    ]
