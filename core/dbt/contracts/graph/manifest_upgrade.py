from dbt import deprecations
from dbt.dataclass_schema import ValidationError


# we renamed these properties in v1.3
# this method allows us to be nice to the early adopters
def rename_metric_attr(data: dict, raise_deprecation_warning: bool = False) -> dict:
    metric_name = data["name"]
    if raise_deprecation_warning and (
        "sql" in data.keys()
        or "type" in data.keys()
        or data.get("calculation_method") == "expression"
    ):
        deprecations.warn("metric-attr-renamed", metric_name=metric_name)
    duplicated_attribute_msg = """\n
The metric '{}' contains both the deprecated metric property '{}'
and the up-to-date metric property '{}'. Please remove the deprecated property.
"""
    if "sql" in data.keys():
        if "expression" in data.keys():
            raise ValidationError(
                duplicated_attribute_msg.format(metric_name, "sql", "expression")
            )
        else:
            data["expression"] = data.pop("sql")
    if "type" in data.keys():
        if "calculation_method" in data.keys():
            raise ValidationError(
                duplicated_attribute_msg.format(metric_name, "type", "calculation_method")
            )
        else:
            calculation_method = data.pop("type")
            data["calculation_method"] = calculation_method
    # we also changed "type: expression" -> "calculation_method: derived"
    if data.get("calculation_method") == "expression":
        data["calculation_method"] = "derived"
    return data


def rename_sql_attr(node_content: dict) -> dict:
    if "raw_sql" in node_content:
        node_content["raw_code"] = node_content.pop("raw_sql")
    if "compiled_sql" in node_content:
        node_content["compiled_code"] = node_content.pop("compiled_sql")
    node_content["language"] = "sql"
    return node_content


def upgrade_ref_content(node_content: dict) -> dict:
    # In v1.5 we switched Node.refs from List[List[str]] to List[Dict[str, Union[NodeVersion, str]]]
    # Previous versions did not have a version keyword argument for ref
    if "refs" in node_content:
        upgraded_refs = []
        for ref in node_content["refs"]:
            if isinstance(ref, list):
                if len(ref) == 1:
                    upgraded_refs.append({"package": None, "name": ref[0], "version": None})
                else:
                    upgraded_refs.append({"package": ref[0], "name": ref[1], "version": None})
        node_content["refs"] = upgraded_refs
    return node_content


def upgrade_node_content(node_content):
    rename_sql_attr(node_content)
    upgrade_ref_content(node_content)
    if node_content["resource_type"] != "seed" and "root_path" in node_content:
        del node_content["root_path"]


def upgrade_seed_content(node_content):
    # Remove compilation related attributes
    for attr_name in (
        "language",
        "refs",
        "sources",
        "metrics",
        "compiled_path",
        "compiled",
        "compiled_code",
        "extra_ctes_injected",
        "extra_ctes",
        "relation_name",
    ):
        if attr_name in node_content:
            del node_content[attr_name]
        # In v1.4, we switched SeedNode.depends_on from DependsOn to MacroDependsOn
        node_content.get("depends_on", {}).pop("nodes", None)


def upgrade_manifest_json(manifest: dict) -> dict:
    for node_content in manifest.get("nodes", {}).values():
        upgrade_node_content(node_content)
        if node_content["resource_type"] == "seed":
            upgrade_seed_content(node_content)
    for disabled in manifest.get("disabled", {}).values():
        # There can be multiple disabled nodes for the same unique_id
        # so make sure all the nodes get the attr renamed
        for node_content in disabled:
            upgrade_node_content(node_content)
            if node_content["resource_type"] == "seed":
                upgrade_seed_content(node_content)
    # add group key
    if "groups" not in manifest:
        manifest["groups"] = {}
    if "group_map" not in manifest:
        manifest["group_map"] = {}
    for metric_content in manifest.get("metrics", {}).values():
        # handle attr renames + value translation ("expression" -> "derived")
        metric_content = rename_metric_attr(metric_content)
        metric_content = upgrade_ref_content(metric_content)
        if "root_path" in metric_content:
            del metric_content["root_path"]
    for exposure_content in manifest.get("exposures", {}).values():
        exposure_content = upgrade_ref_content(exposure_content)
        if "root_path" in exposure_content:
            del exposure_content["root_path"]
    for source_content in manifest.get("sources", {}).values():
        if "root_path" in source_content:
            del source_content["root_path"]
    for macro_content in manifest.get("macros", {}).values():
        if "root_path" in macro_content:
            del macro_content["root_path"]
    for doc_content in manifest.get("docs", {}).values():
        if "root_path" in doc_content:
            del doc_content["root_path"]
        doc_content["resource_type"] = "doc"
    return manifest
