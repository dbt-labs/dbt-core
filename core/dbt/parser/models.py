from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.parsed import ParsedModelNode
import dbt.flags as flags
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore


class ModelParser(SimpleSQLParser[ParsedModelNode]):
    def parse_from_dict(self, dct, validate=True) -> ParsedModelNode:
        if validate:
            ParsedModelNode.validate(dct)
        return ParsedModelNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Model

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_update(
        self, node: ParsedModelNode, config: ContextConfig
    ) -> None:
        self.manifest._parsing_info.static_analysis_path_count += 1

        # normal dbt run
        if not flags.USE_EXPERIMENTAL_PARSER:
            super().render_update(node, config)

        # if the --use-experimental-parser flag was set
        else:
            try:
                # run dbt jinja extractor (powered by tree-sitter + rust)
                # throws an exception if it can't parse the source
                res = py_extract_from_source(node.raw_sql)

                # since it doesn't need python jinja, fit the refs, sources, and configs
                # into the node. Down the line the rest of the node will be updated with
                # this information. (e.g. depends_on etc.)
                config_calls = []
                for c in res['configs']:
                    config_calls.append({c[0]: c[1]})

                config._config_calls = config_calls

                # this uses the updated config to set all the right things in the node
                # if there are hooks present, it WILL render jinja. Will need to change
                # when we support hooks
                self.update_parsed_node(node, config)

                # udpate the unrendered config with values from the file
                # values from yaml files are in there already
                node.unrendered_config.update(dict(res['configs']))

                # set refs, sources, and configs on the node object
                node.refs = node.refs + res['refs']
                for sourcev in res['sources']:
                    # TODO change extractor to match type here
                    node.sources.append([sourcev[0], sourcev[1]])
                for configv in res['configs']:
                    node.config[configv[0]] = configv[1]

                self.manifest._parsing_info.static_analysis_parsed_path_count += 1

            # exception was thrown by dbt jinja extractor meaning it can't
            # handle this source. fall back to python jinja rendering.
            except ExtractionError:
                super().render_update(node, config)
