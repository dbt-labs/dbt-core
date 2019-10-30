from threading import local
from typing import Optional, Callable

from dbt.clients.jinja import QueryStringGenerator

# this generates an import cycle, as usual
from dbt.context.base import QueryHeaderContext
from dbt.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.compiled import CompileResultNode


default_query_comment = '''
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
        self.query_comment: str = initial

    def add(self, sql: str) -> str:
        return '/* {} */\n{}'.format(self.query_comment.strip(), sql)

    def set(self, comment: str):
        self.query_comment = comment


QueryStringFunc = Callable[[str, Optional[CompileResultNode]], str]


class QueryStringSetter:
    def __init__(self, config: AdapterRequiredConfig):
        if config.query_comment is not None:
            comment = config.query_comment
        else:
            comment = default_query_comment
        macro = '\n'.join((
            '{%- macro query_comment_macro(connection_name, node) -%}',
            comment,
            '{% endmacro %}'
        ))

        ctx = QueryHeaderContext(config).to_dict()
        self.generator: QueryStringFunc = QueryStringGenerator(macro, ctx)
        self.comment = _QueryComment('')
        self.reset()

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
