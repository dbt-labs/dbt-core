from threading import local
from typing import Optional, Callable, Dict, Any

from dbt.clients.jinja import QueryStringGenerator

from dbt.context.configured import generate_query_header_context
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.exceptions import RuntimeException


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
        self.append = False

    def add(self, sql: str) -> str:
        if not self.query_comment:
            return sql

        if self.append:
            # replace last ';' with '<comment>;'
            sql = sql.rstrip()
            if sql[-1] == ';':
                sql = sql[:-1]
                return '{}\n/* {} */;'.format(sql, self.query_comment.strip())

            return '{}\n/* {} */'.format(sql, self.query_comment.strip())

        return '/* {} */\n{}'.format(self.query_comment.strip(), sql)

    def set(self, comment: Optional[str], append: bool):
        if isinstance(comment, str) and '*/' in comment:
            # tell the user "no" so they don't hurt themselves by writing
            # garbage
            raise RuntimeException(
                f'query comment contains illegal value "*/": {comment}'
            )
        self.query_comment = comment
        self.append = append


QueryStringFunc = Callable[[str, Optional[NodeWrapper]], str]


class MacroQueryStringSetter:
    def __init__(self, config: AdapterRequiredConfig, manifest: Manifest):
        self.manifest = manifest
        self.config = config

        comment_macro = self._get_comment_macro()
        self.generator: QueryStringFunc = lambda name, model: ''
        # if the comment value was None or the empty string, just skip it
        if comment_macro:
            assert isinstance(comment_macro, str)
            macro = '\n'.join((
                '{%- macro query_comment_macro(connection_name, node) -%}',
                comment_macro,
                '{% endmacro %}'
            ))
            ctx = self._get_context()
            self.generator = QueryStringGenerator(macro, ctx)
        self.comment = _QueryComment(None)
        self.reset()

    def _get_comment_macro(self) -> Optional[str]:
        if not self.config.query_comment.get('comment'):
            # query comment is not specified, using default value
            return DEFAULT_QUERY_COMMENT

        comment = self.config.query_comment.get('comment')
        if comment in ('None', ''):
            # query comment is disabled.
            return None

        # using custom query comment
        return comment

    def _get_context(self) -> Dict[str, Any]:
        return generate_query_header_context(self.config, self.manifest)

    def add(self, sql: str) -> str:
        return self.comment.add(sql)

    def reset(self):
        self.set('master', None)

    def set(self, name: str, node: Optional[CompileResultNode]):
        wrapped: Optional[NodeWrapper] = None
        if node is not None:
            wrapped = NodeWrapper(node)
        comment_str = self.generator(name, wrapped)

        self.comment.set(
            comment_str, self.config.query_comment.get('append', False))
