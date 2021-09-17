from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.parsed import ParsedModelNode
import dbt.flags as flags
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock
import dbt.tracking as tracking
from dbt import utils
from dbt_extractor import ExtractionError, py_extract_from_source  # type: ignore
from functools import reduce
from itertools import chain
import random
from typing import Any, Dict, Iterator, List, Optional, Union


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

    # TODO when this is turned on by default, simplify the nasty if/else tree inside this method.
    def render_update(
        self, node: ParsedModelNode, config: ContextConfig
    ) -> None:
        # TODO go back to 1/100 when this is turned on by default.
        # `True` roughly 1/50 times this function is called
        sample: bool = random.randint(1, 51) == 50

        # top-level declaration of variables
        experimentally_parsed: Optional[Union[str, Dict[str, List[Any]]]] = None
        config_call_dict: Dict[str, Any] = {}
        source_calls: List[List[str]] = []

        # run the experimental parser if the flag is on or if we're sampling
        if flags.USE_EXPERIMENTAL_PARSER or sample:
            if self._has_banned_macro(node):
                # this log line is used for integration testing. If you change
                # the code at the beginning of the line change the tests in
                # test/integration/072_experimental_parser_tests/test_all_experimental_parser.py
                logger.debug(
                    f"1601: parser fallback to jinja because of macro override for {node.path}"
                )
                experimentally_parsed = "has_banned_macro"
            else:
                # run the experimental parser and return the results
                try:
                    experimentally_parsed = py_extract_from_source(
                        node.raw_sql
                    )
                    logger.debug(f"1699: statically parsed {node.path}")
                # if we want information on what features are barring the experimental
                # parser from reading model files, this is where we would add that
                # since that information is stored in the `ExtractionError`.
                except ExtractionError:
                    experimentally_parsed = "cannot_parse"

        # if the parser succeeded, extract some data in easy-to-compare formats
        if isinstance(experimentally_parsed, dict):
            # create second config format
            for c in experimentally_parsed['configs']:
                ContextConfig._add_config_call(config_call_dict, {c[0]: c[1]})

            # format sources TODO change extractor to match this type
            for s in experimentally_parsed['sources']:
                source_calls.append([s[0], s[1]])
            experimentally_parsed['sources'] = source_calls

        # normal dbt run
        if not flags.USE_EXPERIMENTAL_PARSER:
            # normal rendering
            super().render_update(node, config)
            # if we're sampling, compare for correctness
            if sample:
                result = _get_sample_result(
                    experimentally_parsed,
                    config_call_dict,
                    source_calls,
                    node,
                    config
                )
                # fire a tracking event. this fires one event for every sample
                # so that we have data on a per file basis. Not only can we expect
                # no false positives or misses, we can expect the number model
                # files parseable by the experimental parser to match our internal
                # testing.
                if tracking.active_user is not None:  # None in some tests
                    tracking.track_experimental_parser_sample({
                        "project_id": self.root_project.hashed_name(),
                        "file_id": utils.get_hash(node),
                        "status": result
                    })

        # if the --use-experimental-parser flag was set, and the experimental parser succeeded
        elif isinstance(experimentally_parsed, Dict):
            # since it doesn't need python jinja, fit the refs, sources, and configs
            # into the node. Down the line the rest of the node will be updated with
            # this information. (e.g. depends_on etc.)
            config._config_call_dict = config_call_dict

            # this uses the updated config to set all the right things in the node.
            # if there are hooks present, it WILL render jinja. Will need to change
            # when the experimental parser supports hooks
            self.update_parsed_node_config(node, config)

            # update the unrendered config with values from the file.
            # values from yaml files are in there already
            node.unrendered_config.update(dict(experimentally_parsed['configs']))

            # set refs and sources on the node object
            node.refs += experimentally_parsed['refs']
            node.sources += experimentally_parsed['sources']

            # configs don't need to be merged into the node
            # setting them in config._config_call_dict is sufficient

            self.manifest._parsing_info.static_analysis_parsed_path_count += 1

        # the experimental parser didn't run on this model.
        # fall back to python jinja rendering.
        elif experimentally_parsed in ["has_banned_macro"]:
            # not logging here since the reason should have been logged above
            super().render_update(node, config)
        # the experimental parser ran on this model and failed.
        # fall back to python jinja rendering.
        else:
            logger.debug(
                f"1602: parser fallback to jinja because of extractor failure for {node.path}"
            )
            super().render_update(node, config)

    # checks for banned macros
    def _has_banned_macro(
        self, node: ParsedModelNode
    ) -> bool:
        # first check if there is a banned macro defined in scope for this model file
        root_project_name = self.root_project.project_name
        project_name = node.package_name
        banned_macros = ['ref', 'source', 'config']

        all_banned_macro_keys: Iterator[str] = chain.from_iterable(
            map(
                lambda name: [
                    f"macro.{project_name}.{name}",
                    f"macro.{root_project_name}.{name}"
                ],
                banned_macros
            )
        )

        return reduce(
            lambda z, key: z or (key in self.manifest.macros),
            all_banned_macro_keys,
            False
        )


# returns a list of string codes to be sent as a tracking event
def _get_sample_result(
    sample_output: Optional[Union[str, Dict[str, Any]]],
    config_call_dict: Dict[str, Any],
    source_calls: List[List[str]],
    node: ParsedModelNode,
    config: ContextConfig
) -> List[str]:
    result: List[str] = []
    # experimental parser didn't run
    if sample_output is None:
        result += ["09_experimental_parser_skipped"]
    # experimental parser couldn't parse
    elif (isinstance(sample_output, str)):
        if sample_output == "cannot_parse":
            result += ["01_experimental_parser_cannot_parse"]
        elif sample_output == "has_banned_macro":
            result += ["08_has_banned_macro"]
    else:
        # look for false positive configs
        for k in config_call_dict.keys():
            if k not in config._config_call_dict:
                result += ["02_false_positive_config_value"]
                break

        # look for missed configs
        for k in config._config_call_dict.keys():
            if k not in config_call_dict:
                result += ["03_missed_config_value"]
                break

        # look for false positive sources
        for s in sample_output['sources']:
            if s not in node.sources:
                result += ["04_false_positive_source_value"]
                break

        # look for missed sources
        for s in node.sources:
            if s not in sample_output['sources']:
                result += ["05_missed_source_value"]
                break

        # look for false positive refs
        for r in sample_output['refs']:
            if r not in node.refs:
                result += ["06_false_positive_ref_value"]
                break

        # look for missed refs
        for r in node.refs:
            if r not in sample_output['refs']:
                result += ["07_missed_ref_value"]
                break

        # if there are no errors, return a success value
        if not result:
            result = ["00_exact_match"]

    return result
