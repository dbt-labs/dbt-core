from copy import deepcopy
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
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union


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

        if not flags.STATIC_PARSER:
            # jinja rendering
            super().render_update(node, config)
            logger.debug(f"1605: jinja rendering because of STATIC_PARSER flag. file: {node.path}")
            return

        # only sample for experimental parser correctness on normal runs,
        # not when the experimental parser flag is on.
        exp_sample: bool = False
        # sampling the stable static parser against jinja is significantly
        # more expensive and therefor done far less frequently.
        stable_sample: bool = False
        # there are two samples above, and it is perfectly fine if both happen
        # at the same time. If that happens, the experimental parser, stable
        # parser, and jinja rendering will run on the same model file and
        # send back codes for experimental v stable, and stable v jinja.
        if not flags.USE_EXPERIMENTAL_PARSER:
            # `True` roughly 1/5000 times this function is called
            # sample = random.randint(1, 5001) == 5000
            stable_sample = random.randint(1, 5001) == 5000
            # sampling the experimental parser is explicitly disabled here, but use the following
            # commented code to sample a fraction of the time when new
            # experimental features are added.
            # `True` roughly 1/100 times this function is called
            # exp_sample = random.randint(1, 101) == 100

        # top-level declaration of variables
        statically_parsed: Optional[Union[str, Dict[str, List[Any]]]] = None
        experimental_sample: Optional[Union[str, Dict[str, List[Any]]]] = None
        exp_sample_node: Optional[ParsedModelNode] = None
        exp_sample_config: Optional[ContextConfig] = None
        jinja_sample_node: Optional[ParsedModelNode] = None
        jinja_sample_config: Optional[ContextConfig] = None
        result: List[str] = []

        # sample the experimental parser only during a normal run
        if exp_sample and not flags.USE_EXPERIMENTAL_PARSER:
            logger.debug(f"1610: conducting experimental parser sample on {node.path}")
            experimental_sample = self.run_experimental_parser(node)
            # if the experimental parser succeeded, make a full copy of model parser
            # and populate _everything_ into it so it can be compared apples-to-apples
            # with a fully jinja-rendered project. This is necessary because the experimental
            # parser will likely add features that the existing static parser will fail on
            # so comparing those directly would give us bad results. The comparison will be
            # conducted after this model has been fully rendered either by the static parser
            # or by full jinja rendering
            if isinstance(experimental_sample, dict):
                model_parser_copy = self.partial_deepcopy()
                exp_sample_node = deepcopy(node)
                exp_sample_config = deepcopy(config)
                model_parser_copy.populate(
                    exp_sample_node,
                    exp_sample_config,
                    experimental_sample['refs'],
                    experimental_sample['sources'],
                    dict(experimental_sample['configs'])
                )
        # use the experimental parser exclusively if the flag is on
        if flags.USE_EXPERIMENTAL_PARSER:
            statically_parsed = self.run_experimental_parser(node)
        # run the stable static parser unless it is explicitly turned off
        else:
            statically_parsed = self.run_static_parser(node)

        # if the static parser succeeded, extract some data in easy-to-compare formats
        if isinstance(statically_parsed, dict):
            # only sample jinja for the purpose of comparing with the stable static parser
            # if we know we don't need to fall back to jinja (i.e. - nothing to compare
            # with jinja v jinja).
            # This means we skip sampling for 40% of the 1/5000 samples. We could run the
            # sampling rng here, but the effect would be the same since we would only roll
            # it 40% of the time. So I've opted to keep all the rng code colocated above.
            if stable_sample and not flags.USE_EXPERIMENTAL_PARSER:
                logger.debug(f"1611: conducting full jinja rendering sample on {node.path}")
                # if this will _never_ mutate anything `self` we could avoid these deep copies,
                # but we can't really guarantee that going forward.
                model_parser_copy = self.partial_deepcopy()
                jinja_sample_node = deepcopy(node)
                jinja_sample_config = deepcopy(config)
                # rendering mutates the node and the config
                super(ModelParser, model_parser_copy) \
                    .render_update(jinja_sample_node, jinja_sample_config)

            # manually fit configs in
            config._config_call_dict = _get_config_call_dict(statically_parsed)

            # update the unrendered config with values from the static parser.
            # values from yaml files are in there already
            self.populate(
                node,
                config,
                statically_parsed['refs'],
                statically_parsed['sources'],
                dict(statically_parsed['configs'])
            )

            # if we took a jinja sample, compare now that the base node has been populated
            if jinja_sample_node is not None and jinja_sample_config is not None:
                result = _get_stable_sample_result(
                    jinja_sample_node,
                    jinja_sample_config,
                    node,
                    config
                )

            # if we took an experimental sample, compare now that the base node has been populated
            if exp_sample_node is not None and exp_sample_config is not None:
                result = _get_exp_sample_result(
                    exp_sample_node,
                    exp_sample_config,
                    node,
                    config,
                )

            self.manifest._parsing_info.static_analysis_parsed_path_count += 1
        # if the static parser failed, add the correct messages for tracking
        elif isinstance(statically_parsed, str):
            if statically_parsed == "cannot_parse":
                result += ["01_stable_parser_cannot_parse"]
            elif statically_parsed == "has_banned_macro":
                result += ["08_has_banned_macro"]

            super().render_update(node, config)
            logger.debug(
                f"1602: parser fallback to jinja rendering on {node.path}"
            )
        # if the static parser didn't succeed, fall back to jinja
        else:
            # jinja rendering
            super().render_update(node, config)
            logger.debug(
                f"1602: parser fallback to jinja rendering on {node.path}"
            )

        # only send the tracking event if there is at least one result code
        if result:
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

    def run_static_parser(
        self, node: ParsedModelNode
    ) -> Optional[Union[str, Dict[str, List[Any]]]]:
        # if any banned macros have been overridden by the user, we cannot use the static parser.
        if self._has_banned_macro(node):
            # this log line is used for integration testing. If you change
            # the code at the beginning of the line change the tests in
            # test/integration/072_experimental_parser_tests/test_all_experimental_parser.py
            logger.debug(
                f"1601: detected macro override of ref/source/config in the scope of {node.path}"
            )
            return "has_banned_macro"

        # run the stable static parser and return the results
        try:
            statically_parsed = py_extract_from_source(
                node.raw_sql
            )
            logger.debug(f"1699: static parser successfully parsed {node.path}")
            return _shift_sources(statically_parsed)
        # if we want information on what features are barring the static
        # parser from reading model files, this is where we would add that
        # since that information is stored in the `ExtractionError`.
        except ExtractionError:
            logger.debug(
                f"1603: static parser failed on {node.path}"
            )
            return "cannot_parse"

    def run_experimental_parser(
        self, node: ParsedModelNode
    ) -> Optional[Union[str, Dict[str, List[Any]]]]:
        # if any banned macros have been overridden by the user, we cannot use the static parser.
        if self._has_banned_macro(node):
            # this log line is used for integration testing. If you change
            # the code at the beginning of the line change the tests in
            # test/integration/072_experimental_parser_tests/test_all_experimental_parser.py
            logger.debug(
                f"1601: detected macro override of ref/source/config in the scope of {node.path}"
            )
            return "has_banned_macro"

        # run the experimental parser and return the results
        try:
            # for now, this line calls the stable static parser since there are no
            # experimental features. Change `py_extract_from_source` to the new
            # experimental call when we add additional features.
            experimentally_parsed = py_extract_from_source(
                node.raw_sql
            )
            logger.debug(f"1698: experimental parser successfully parsed {node.path}")
            return _shift_sources(experimentally_parsed)
        # if we want information on what features are barring the experimental
        # parser from reading model files, this is where we would add that
        # since that information is stored in the `ExtractionError`.
        except ExtractionError:
            logger.debug(
                f"1604: experimental parser failed on {node.path}"
            )
            return "cannot_parse"

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

    # this method updates the model note rendered and unrendered config as well
    # as the node object. Used to populate these values when circumventing jinja
    # rendering like the static parser.
    def populate(
        self,
        node: ParsedModelNode,
        config: ContextConfig,
        refs: List[List[str]],
        sources: List[List[str]],
        configs: Dict[str, Any]
    ):
        # if there are hooks present this, it WILL render jinja. Will need to change
        # when the experimental parser supports hooks
        self.update_parsed_node_config(node, config)

        # update the unrendered config with values from the file.
        # values from yaml files are in there already
        node.unrendered_config.update(configs)

        # set refs and sources on the node object
        node.refs += refs
        node.sources += sources

        # configs don't need to be merged into the node because they
        # are read from config._config_call_dict

    # the manifest is often huge so this method avoids deepcopying it
    def partial_deepcopy(self):
        return ModelParser(
            deepcopy(self.project),
            self.manifest,
            deepcopy(self.root_project)
        )


# pure function. safe to use elsewhere, but unlikely to be useful outside this file.
def _get_config_call_dict(
    static_parser_result: Dict[str, List[Any]]
) -> Dict[str, Any]:
    config_call_dict: Dict[str, Any] = {}

    for c in static_parser_result['configs']:
        ContextConfig._add_config_call(config_call_dict, {c[0]: c[1]})

    return config_call_dict


# TODO if we format sources in the extractor to match this type, we won't need this function.
def _shift_sources(
    static_parser_result: Dict[str, List[Any]]
) -> Dict[str, List[Any]]:
    shifted_result = deepcopy(static_parser_result)
    source_calls = []

    for s in static_parser_result['sources']:
        source_calls.append([s[0], s[1]])
    shifted_result['sources'] = source_calls

    return shifted_result


# returns a list of string codes to be sent as a tracking event
def _get_exp_sample_result(
    sample_node: ParsedModelNode,
    sample_config: ContextConfig,
    node: ParsedModelNode,
    config: ContextConfig
) -> List[str]:
    result: List[Tuple[int, str]] = _get_sample_result(sample_node, sample_config, node, config)

    def process(codemsg):
        code, msg = codemsg
        return f"0{code}_experimental_{msg}"

    return list(map(process, result))


# returns a list of string codes to be sent as a tracking event
def _get_stable_sample_result(
    sample_node: ParsedModelNode,
    sample_config: ContextConfig,
    node: ParsedModelNode,
    config: ContextConfig
) -> List[str]:
    result: List[Tuple[int, str]] = _get_sample_result(sample_node, sample_config, node, config)

    def process(codemsg):
        code, msg = codemsg
        return f"8{code}_stable_{msg}"

    return list(map(process, result))


# returns a list of string codes that need a single digit prefix to be prepended
# before being sent as a tracking event
def _get_sample_result(
    sample_node: ParsedModelNode,
    sample_config: ContextConfig,
    node: ParsedModelNode,
    config: ContextConfig
) -> List[Tuple[int, str]]:
    result: List[Tuple[int, str]] = []
    # look for false positive configs
    for k in config._config_call_dict:
        if k not in config._config_call_dict:
            result += [(2, "false_positive_config_value")]
            break

    # look for missed configs
    for k in config._config_call_dict.keys():
        if k not in sample_config._config_call_dict.keys():
            result += [(3, "missed_config_value")]
            break

    # look for false positive sources
    for s in sample_node.sources:
        if s not in node.sources:
            result += [(4, "false_positive_source_value")]
            break

    # look for missed sources
    for s in node.sources:
        if s not in sample_node.sources:
            result += [(5, "missed_source_value")]
            break

    # look for false positive refs
    for r in sample_node.refs:
        if r not in node.refs:
            result += [(6, "false_positive_ref_value")]
            break

    # look for missed refs
    for r in node.refs:
        if r not in sample_node.refs:
            result += [(7, "missed_ref_value")]
            break

    # if there are no errors, return a success value
    if not result:
        result = [(0, "exact_match")]

    return result
