from typing import Dict, Any
from dbt_common.clients.jinja import BaseMacroGenerator, get_environment


class QueryStringGenerator(BaseMacroGenerator):
    def __init__(self, template_str: str, context: Dict[str, Any]) -> None:
        super().__init__(context)
        self.template_str: str = template_str
        env = get_environment()
        self.template = env.from_string(
            self.template_str,
            globals=self.context,
        )

    def get_name(self) -> str:
        return "query_comment_macro"

    def get_template(self):
        """Don't use the template cache, we don't have a node"""
        return self.template

    def __call__(self, connection_name: str, node) -> str:
        return str(self.call_macro(connection_name, node))
