from threading import local
from typing import Optional, Callable

from dbt.clients.jinja import QueryStringGenerator

# this generates an import cycle, as usual
from dbt.context.base import QueryHeaderContext
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException
from dbt.helper_types import NoValue


DEFAULT_QUERY_COMMENT = '''
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}
{% else %}
  {# in the node context, the connection name is the node_id #}
  {%- do comment_dict.update(connection_name=connection_name) -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
'''


class NodeWrapper:
    def __init__(self, node):
        self._inner_node = node

    def __getattr__(self, name):
        return getattr(self._inner_node, name, '')


class _QueryComment(local):
    """A thread-local class storing thread-specific state information for
    connection management, namely:
        - the current thread's query comment.
        - a source_name indicating what set the current thread's query comment
    """
    def __init__(self, initial):
        self.query_comment: Optional[str] = initial

    def add(self, sql: str) -> str:
        if not self.query_comment:
            return sql
        else:
            return '/* {} */\n{}'.format(self.query_comment.strip(), sql)

    def set(self, comment: Optional[str]):
        if '*/' in comment:
            # tell the user "no" so they don't hurt themselves by writing
            # garbage
            raise RuntimeException(
                f'query comment contains illegal value "*/": {comment}'
            )
        self.query_comment = comment


QueryStringFunc = Callable[[str, Optional[CompileResultNode]], str]


class QueryStringSetter:
    """The base query string setter. This is only used once."""
    def __init__(self, config: AdapterRequiredConfig):
        self.config = config

        comment_macro = self._get_comment_macro()
        self.generator: QueryStringFunc = lambda name, model: ''
        # if the comment value was None or the empty string, just skip it
        if comment_macro:
            macro = '\n'.join((
                '{%- macro query_comment_macro(connection_name, node) -%}',
                self._get_comment_macro(),
                '{% endmacro %}'
            ))
            ctx = self._get_context()
            self.generator: QueryStringFunc = QueryStringGenerator(macro, ctx)
        self.comment = _QueryComment(None)
        self.reset()

    def _get_context(self):
        return QueryHeaderContext(self.config).to_dict()

    def _get_comment_macro(self) -> Optional[str]:
        # if the query comment is null/empty string, there is no comment at all
        if not self.config.query_comment:
            return None
        else:
            # else, the default
            return DEFAULT_QUERY_COMMENT

    def add(self, sql: str) -> str:
        return self.comment.add(sql)

    def reset(self):
        self.set('master', None)

    def set(self, name: str, node: Optional[CompileResultNode]):
        if node is not None:
            wrapped = NodeWrapper(node)
        else:
            wrapped = None
        comment_str = self.generator(name, wrapped)
        self.comment.set(comment_str)


class MacroQueryStringSetter(QueryStringSetter):
    def __init__(self, config: AdapterRequiredConfig, manifest: Manifest):
        self.manifest = manifest
        super().__init__(config)

    def _get_comment_macro(self):
        if (
            self.config.query_comment != NoValue() and
            self.config.query_comment
        ):
            return self.config.query_comment
        else:
            return super()._get_comment_macro()

    def _get_context(self):
        return QueryHeaderContext(self.config).to_dict(self.manifest.macros)
