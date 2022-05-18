import os
from copy import deepcopy
from typing import MutableMapping, Dict, List
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import (
    AnySourceFile,
    ParseFileType,
    parse_file_type_to_parser,
)
from dbt.events.functions import fire_event
from dbt.events.types import (
    PartialParsingEnabled,
    PartialParsingAddedFile,
    PartialParsingDeletedFile,
    PartialParsingUpdatedFile,
    PartialParsingNodeMissingInSourceFile,
    PartialParsingMissingNodes,
    PartialParsingChildMapMissingUniqueID,
    PartialParsingUpdateSchemaFile,
    PartialParsingDeletedSource,
    PartialParsingDeletedExposure,
    PartialParsingDeletedMetric,
)
from dbt.node_types import NodeType


mssat_files = (
    ParseFileType.Model,
    ParseFileType.Seed,
    ParseFileType.Snapshot,
    ParseFileType.Analysis,
    ParseFileType.SingularTest,
)

mg_files = (
    ParseFileType.Macro,
    ParseFileType.GenericTest,
)


key_to_prefix = {
    "models": "model",
    "seeds": "seed",
    "snapshots": "snapshot",
    "analyses": "analysis",
}


parse_file_type_to_key = {
    ParseFileType.Model: "models",
    ParseFileType.Seed: "seeds",
    ParseFileType.Snapshot: "snapshots",
    ParseFileType.Analysis: "analyses",
}


# These macro names have special treatment in the ManifestLoader and
# partial parsing. If they have changed we will skip partial parsing
special_override_macros = [
    "ref",
    "source",
    "config",
    "generate_schema_name",
    "generate_database_name",
    "generate_alias_name",
]


# Partial parsing. Create a diff of files from saved manifest and current
# files and produce a project_parser_file dictionary to drive parsing of
# only the necessary changes.
# Will produce a 'skip_parsing' method, and a project_parser_file dictionary
# All file objects from the new manifest are deepcopied, because we need
# to preserve an unchanged file object in case we need to drop back to a
# a full parse (such as for certain macro changes)
class PartialParsing:
    def __init__(self, saved_manifest: Manifest, new_files: MutableMapping[str, AnySourceFile]):
        self.saved_manifest = saved_manifest
        self.new_files = new_files
        self.project_parser_files: Dict = {}
        self.saved_files = self.saved_manifest.files
        self.project_parser_files = {}
        self.deleted_manifest = Manifest()
        self.macro_child_map: Dict[str, List[str]] = {}
        (
            self.env_vars_changed_source_files,
            self.env_vars_changed_schema_files,
        ) = self.build_env_vars_to_files()
        self.build_file_diff()
        self.processing_file = None
        self.deleted_special_override_macro = False
        self.disabled_by_file_id = self.saved_manifest.build_disabled_by_file_id()

    def skip_parsing(self):
        return (
            not self.file_diff["deleted"]
            and not self.file_diff["added"]
            and not self.file_diff["changed"]
            and not self.file_diff["changed_schema_files"]
            and not self.file_diff["deleted_schema_files"]
        )

    # Compare the previously saved manifest files and the just-loaded manifest
    # files to see if anything changed
    def build_file_diff(self):
        saved_file_ids = set(self.saved_files.keys())
        new_file_ids = set(self.new_files.keys())
        deleted_all_files = saved_file_ids.difference(new_file_ids)
        added = new_file_ids.difference(saved_file_ids)
        common = saved_file_ids.intersection(new_file_ids)
        changed_or_deleted_macro_file = False

        # separate out deleted schema files
        deleted_schema_files = []
        deleted = []
        for file_id in deleted_all_files:
            if self.saved_files[file_id].parse_file_type == ParseFileType.Schema:
                deleted_schema_files.append(file_id)
            else:
                if self.saved_files[file_id].parse_file_type in mg_files:
                    changed_or_deleted_macro_file = True
                deleted.append(file_id)

        changed = []
        changed_schema_files = []
        unchanged = []
        for file_id in common:
            if self.saved_files[file_id].checksum == self.new_files[file_id].checksum:
                unchanged.append(file_id)
            else:
                # separate out changed schema files
                if self.saved_files[file_id].parse_file_type == ParseFileType.Schema:
                    sf = self.saved_files[file_id]
                    if type(sf).__name__ != "SchemaSourceFile":
                        raise Exception(f"Serialization failure for {file_id}")
                    changed_schema_files.append(file_id)
                else:
                    if self.saved_files[file_id].parse_file_type in mg_files:
                        changed_or_deleted_macro_file = True
                    changed.append(file_id)

        # handle changed env_vars for non-schema-files
        for file_id in self.env_vars_changed_source_files:
            if file_id in deleted or file_id in changed:
                continue
            changed.append(file_id)

        # handle changed env_vars for schema files
        for file_id in self.env_vars_changed_schema_files.keys():
            if file_id in deleted_schema_files or file_id in changed_schema_files:
                continue
            changed_schema_files.append(file_id)

        file_diff = {
            "deleted": deleted,
            "deleted_schema_files": deleted_schema_files,
            "added": added,
            "changed": changed,
            "changed_schema_files": changed_schema_files,
            "unchanged": unchanged,
        }
        if changed_or_deleted_macro_file:
            self.macro_child_map = self.saved_manifest.build_macro_child_map()
        deleted = len(deleted) + len(deleted_schema_files)
        changed = len(changed) + len(changed_schema_files)
        fire_event(PartialParsingEnabled(deleted=deleted, added=len(added), changed=changed))
        self.file_diff = file_diff

    # generate the list of files that need parsing
    # uses self.manifest.files generated by 'read_files'
    def get_parsing_files(self):
        if self.skip_parsing():
            return {}
        # Need to add new files first, because changes in schema files
        # might refer to them
        for file_id in self.file_diff["added"]:
            self.processing_file = file_id
            self.add_to_saved(file_id)
        # Need to process schema files next, because the dictionaries
        # need to be in place for handling SQL file changes
        for file_id in self.file_diff["changed_schema_files"]:
            self.processing_file = file_id
            self.change_schema_file(file_id)
        for file_id in self.file_diff["deleted_schema_files"]:
            self.processing_file = file_id
            self.delete_schema_file(file_id)
        for file_id in self.file_diff["deleted"]:
            self.processing_file = file_id
            self.delete_from_saved(file_id)
        for file_id in self.file_diff["changed"]:
            self.processing_file = file_id
            self.update_in_saved(file_id)
        return self.project_parser_files

    # Add the file to the project parser dictionaries to schedule parsing
    def add_to_pp_files(self, source_file):
        file_id = source_file.file_id
        parser_name = parse_file_type_to_parser[source_file.parse_file_type]
        project_name = source_file.project_name
        if not parser_name or not project_name:
            raise Exception(
                f"Did not find parse_file_type or project_name "
                f"in SourceFile for {source_file.file_id}"
            )
        if project_name not in self.project_parser_files:
            self.project_parser_files[project_name] = {}
        if parser_name not in self.project_parser_files[project_name]:
            self.project_parser_files[project_name][parser_name] = []
        if (
            file_id not in self.project_parser_files[project_name][parser_name]
            and file_id not in self.file_diff["deleted"]
        ):
            self.project_parser_files[project_name][parser_name].append(file_id)

    def already_scheduled_for_parsing(self, source_file):
        file_id = source_file.file_id
        project_name = source_file.project_name
        if project_name not in self.project_parser_files:
            return False
        parser_name = parse_file_type_to_parser[source_file.parse_file_type]
        if parser_name not in self.project_parser_files[project_name]:
            return False
        if file_id not in self.project_parser_files[project_name][parser_name]:
            return False
        return True

    # Add new files, including schema files
    def add_to_saved(self, file_id):
        # add file object to saved manifest.files
        source_file = deepcopy(self.new_files[file_id])
        if source_file.parse_file_type == ParseFileType.Schema:
            self.handle_added_schema_file(source_file)
        self.saved_files[file_id] = source_file
        # update pp_files to parse
        self.add_to_pp_files(source_file)
        fire_event(PartialParsingAddedFile(file_id=file_id))

    def handle_added_schema_file(self, source_file):
        source_file.pp_dict = source_file.dict_from_yaml.copy()
        if "sources" in source_file.pp_dict:
            for source in source_file.pp_dict["sources"]:
                # We need to remove the original source, so it can
                # be properly patched
                if "overrides" in source:
                    self.remove_source_override_target(source)

    # Deletes for all non-schema files
    def delete_from_saved(self, file_id):
        # Look at all things touched by file, remove those
        # nodes, and update pp_files to parse unless the
        # file creating those nodes has also been deleted
        saved_source_file = self.saved_files[file_id]

        # SQL file: models, seeds, snapshots, analyses, tests: SQL files, except
        # macros/tests
        if saved_source_file.parse_file_type in mssat_files:
            self.remove_mssat_file(saved_source_file)
            self.deleted_manifest.files[file_id] = self.saved_manifest.files.pop(file_id)

        # macros
        if saved_source_file.parse_file_type in mg_files:
            self.delete_macro_file(saved_source_file, follow_references=True)

        # docs
        if saved_source_file.parse_file_type == ParseFileType.Documentation:
            self.delete_doc_node(saved_source_file)

        fire_event(PartialParsingDeletedFile(file_id=file_id))

    # Updates for non-schema files
    def update_in_saved(self, file_id):
        new_source_file = deepcopy(self.new_files[file_id])
        old_source_file = self.saved_files[file_id]

        if new_source_file.parse_file_type in mssat_files:
            self.update_mssat_in_saved(new_source_file, old_source_file)
        elif new_source_file.parse_file_type in mg_files:
            self.update_macro_in_saved(new_source_file, old_source_file)
        elif new_source_file.parse_file_type == ParseFileType.Documentation:
            self.update_doc_in_saved(new_source_file, old_source_file)
        else:
            raise Exception(f"Invalid parse_file_type in source_file {file_id}")
        fire_event(PartialParsingUpdatedFile(file_id=file_id))

    # Models, seeds, snapshots: patches and tests
    # analyses: patches, no tests
    # tests: not touched by schema files (no patches, no tests)
    # Updated schema files should have been processed already.
    def update_mssat_in_saved(self, new_source_file, old_source_file):

        if self.already_scheduled_for_parsing(old_source_file):
            return

        # These files only have one node except for snapshots
        unique_ids = []
        if old_source_file.nodes:
            unique_ids = old_source_file.nodes
        else:
            # It's not clear when this would actually happen.
            # Logging in case there are other associated errors.
            fire_event(PartialParsingNodeMissingInSourceFile(source_file=old_source_file))

        # replace source_file in saved and add to parsing list
        file_id = new_source_file.file_id
        self.deleted_manifest.files[file_id] = old_source_file
        self.saved_files[file_id] = deepcopy(new_source_file)
        self.add_to_pp_files(new_source_file)
        for unique_id in unique_ids:
            self.remove_node_in_saved(new_source_file, unique_id)

    def remove_node_in_saved(self, source_file, unique_id):
        if unique_id in self.saved_manifest.nodes:
            # delete node in saved
            node = self.saved_manifest.nodes.pop(unique_id)
            self.deleted_manifest.nodes[unique_id] = node
        elif (
            source_file.file_id in self.disabled_by_file_id
            and unique_id in self.saved_manifest.disabled
        ):
            # This node is disabled. Find the node and remove it from disabled dictionary.
            for dis_index, dis_node in enumerate(self.saved_manifest.disabled[unique_id]):
                if dis_node.file_id == source_file.file_id:
                    node = dis_node
                    break
            if dis_node:
                # Remove node from disabled and unique_id from disabled dict if necessary
                del self.saved_manifest.disabled[unique_id][dis_index]
                if not self.saved_manifest.disabled[unique_id]:
                    self.saved_manifest.disabled.pop(unique_id)
        else:
            # Has already been deleted by another action
            return

        # look at patch_path in model node to see if we need
        # to reapply a patch from a schema_file.
        if node.patch_path:
            file_id = node.patch_path
            # it might be changed...  then what?
            if file_id not in self.file_diff["deleted"] and file_id in self.saved_files:
                # schema_files should already be updated
                schema_file = self.saved_files[file_id]
                dict_key = parse_file_type_to_key[source_file.parse_file_type]
                # look for a matching list dictionary
                elem_patch = None
                if dict_key in schema_file.dict_from_yaml:
                    for elem in schema_file.dict_from_yaml[dict_key]:
                        if elem["name"] == node.name:
                            elem_patch = elem
                            break
                if elem_patch:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem_patch)
                    self.merge_patch(schema_file, dict_key, elem_patch)
                    if unique_id in schema_file.node_patches:
                        schema_file.node_patches.remove(unique_id)
            if unique_id in self.saved_manifest.disabled:
                # We have a patch_path in disabled nodes with a patch so
                # that we can connect the patch to the node
                for node in self.saved_manifest.disabled[unique_id]:
                    node.patch_path = None

    def update_macro_in_saved(self, new_source_file, old_source_file):
        if self.already_scheduled_for_parsing(old_source_file):
            return
        self.handle_macro_file_links(old_source_file, follow_references=True)
        file_id = new_source_file.file_id
        self.saved_files[file_id] = deepcopy(new_source_file)
        self.add_to_pp_files(new_source_file)

    def update_doc_in_saved(self, new_source_file, old_source_file):
        if self.already_scheduled_for_parsing(old_source_file):
            return
        self.delete_doc_node(old_source_file)
        self.saved_files[new_source_file.file_id] = deepcopy(new_source_file)
        self.add_to_pp_files(new_source_file)

    def remove_mssat_file(self, source_file):
        # nodes [unique_ids] -- SQL files
        # There should always be a node for a SQL file
        if not source_file.nodes:
            fire_event(PartialParsingMissingNodes(file_id=source_file.file_id))
            return
        # There is generally only 1 node for SQL files, except for macros and snapshots
        for unique_id in source_file.nodes:
            self.remove_node_in_saved(source_file, unique_id)
            self.schedule_referencing_nodes_for_parsing(unique_id)

    # We need to re-parse nodes that reference another removed node
    def schedule_referencing_nodes_for_parsing(self, unique_id):
        # Look at "children", i.e. nodes that reference this node
        if unique_id in self.saved_manifest.child_map:
            self.schedule_nodes_for_parsing(self.saved_manifest.child_map[unique_id])
        else:
            fire_event(PartialParsingChildMapMissingUniqueID(unique_id=unique_id))

    def schedule_nodes_for_parsing(self, unique_ids):
        for unique_id in unique_ids:
            if unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes[unique_id]
                if node.resource_type == NodeType.Test and node.test_node_type == "generic":
                    # test nodes are handled separately. Must be removed from schema file
                    continue
                file_id = node.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    source_file = self.saved_files[file_id]
                    self.remove_mssat_file(source_file)
                    # content of non-schema files is only in new files
                    self.saved_files[file_id] = deepcopy(self.new_files[file_id])
                    self.add_to_pp_files(self.saved_files[file_id])
            elif unique_id in self.saved_manifest.sources:
                source = self.saved_manifest.sources[unique_id]
                file_id = source.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    schema_file = self.saved_files[file_id]
                    sources = []
                    if "sources" in schema_file.dict_from_yaml:
                        sources = schema_file.dict_from_yaml["sources"]
                    source_element = self.get_schema_element(sources, source.source_name)
                    if source_element:
                        self.delete_schema_source(schema_file, source_element)
                        self.remove_tests(schema_file, "sources", source_element["name"])
                        self.merge_patch(schema_file, "sources", source_element)
            elif unique_id in self.saved_manifest.exposures:
                exposure = self.saved_manifest.exposures[unique_id]
                file_id = exposure.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    schema_file = self.saved_files[file_id]
                    exposures = []
                    if "exposures" in schema_file.dict_from_yaml:
                        exposures = schema_file.dict_from_yaml["exposures"]
                    exposure_element = self.get_schema_element(exposures, exposure.name)
                    if exposure_element:
                        self.delete_schema_exposure(schema_file, exposure_element)
                        self.merge_patch(schema_file, "exposures", exposure_element)
            elif unique_id in self.saved_manifest.metrics:
                metric = self.saved_manifest.metrics[unique_id]
                file_id = metric.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    schema_file = self.saved_files[file_id]
                    metrics = []
                    if "metrics" in schema_file.dict_from_yaml:
                        metrics = schema_file.dict_from_yaml["metrics"]
                    metric_element = self.get_schema_element(metrics, metric.name)
                    if metric_element:
                        self.delete_schema_metric(schema_file, metric_element)
                        self.merge_patch(schema_file, "metrics", metric_element)
            elif unique_id in self.saved_manifest.macros:
                macro = self.saved_manifest.macros[unique_id]
                file_id = macro.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    source_file = self.saved_files[file_id]
                    self.delete_macro_file(source_file)
                    self.saved_files[file_id] = deepcopy(self.new_files[file_id])
                    self.add_to_pp_files(self.saved_files[file_id])

    def delete_macro_file(self, source_file, follow_references=False):
        self.check_for_special_deleted_macros(source_file)
        self.handle_macro_file_links(source_file, follow_references)
        file_id = source_file.file_id
        # It's not clear when this file_id would not exist in saved_files
        if file_id in self.saved_files:
            self.deleted_manifest.files[file_id] = self.saved_files.pop(file_id)

    def check_for_special_deleted_macros(self, source_file):
        for unique_id in source_file.macros:
            if unique_id in self.saved_manifest.macros:
                package_name = unique_id.split(".")[1]
                if package_name == "dbt":
                    continue
                macro = self.saved_manifest.macros[unique_id]
                if macro.name in special_override_macros:
                    self.deleted_special_override_macro = True

    def recursively_gather_macro_references(self, macro_unique_id, referencing_nodes):
        for unique_id in self.macro_child_map[macro_unique_id]:
            if unique_id in referencing_nodes:
                continue
            referencing_nodes.append(unique_id)
            if unique_id.startswith("macro."):
                self.recursively_gather_macro_references(unique_id, referencing_nodes)

    def handle_macro_file_links(self, source_file, follow_references=False):
        # remove the macros in the 'macros' dictionary
        macros = source_file.macros.copy()
        for unique_id in macros:
            if unique_id not in self.saved_manifest.macros:
                # This happens when a macro has already been removed
                if unique_id in source_file.macros:
                    source_file.macros.remove(unique_id)
                continue

            base_macro = self.saved_manifest.macros.pop(unique_id)
            self.deleted_manifest.macros[unique_id] = base_macro

            # Recursively check children of this macro
            # The macro_child_map might not exist if a macro is removed by
            # schedule_nodes_for parsing. We only want to follow
            # references if the macro file itself has been updated or
            # deleted, not if we're just updating referenced nodes.
            if self.macro_child_map and follow_references:
                referencing_nodes = []
                self.recursively_gather_macro_references(unique_id, referencing_nodes)
                self.schedule_macro_nodes_for_parsing(referencing_nodes)

            if base_macro.patch_path:
                file_id = base_macro.patch_path
                if file_id in self.saved_files:
                    schema_file = self.saved_files[file_id]
                    macro_patches = []
                    if "macros" in schema_file.dict_from_yaml:
                        macro_patches = schema_file.dict_from_yaml["macros"]
                    macro_patch = self.get_schema_element(macro_patches, base_macro.name)
                    self.delete_schema_macro_patch(schema_file, macro_patch)
                    self.merge_patch(schema_file, "macros", macro_patch)
            # The macro may have already been removed by handling macro children
            if unique_id in source_file.macros:
                source_file.macros.remove(unique_id)

    # similar to schedule_nodes_for_parsing but doesn't do sources and exposures
    # and handles schema tests
    def schedule_macro_nodes_for_parsing(self, unique_ids):
        for unique_id in unique_ids:
            if unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes[unique_id]
                # Both generic tests from yaml files and singular tests have NodeType.Test
                # so check for generic test.
                if node.resource_type == NodeType.Test and node.test_node_type == "generic":
                    schema_file_id = node.file_id
                    schema_file = self.saved_manifest.files[schema_file_id]
                    (key, name) = schema_file.get_key_and_name_for_test(node.unique_id)
                    if key and name:
                        patch_list = []
                        if key in schema_file.dict_from_yaml:
                            patch_list = schema_file.dict_from_yaml[key]
                        patch = self.get_schema_element(patch_list, name)
                        if patch:
                            if key in ["models", "seeds", "snapshots"]:
                                self.delete_schema_mssa_links(schema_file, key, patch)
                                self.merge_patch(schema_file, key, patch)
                                if unique_id in schema_file.node_patches:
                                    schema_file.node_patches.remove(unique_id)
                            elif key == "sources":
                                # re-schedule source
                                if "overrides" in patch:
                                    # This is a source patch; need to re-parse orig source
                                    self.remove_source_override_target(patch)
                                self.delete_schema_source(schema_file, patch)
                                self.remove_tests(schema_file, "sources", patch["name"])
                                self.merge_patch(schema_file, "sources", patch)
                else:
                    file_id = node.file_id
                    if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                        source_file = self.saved_files[file_id]
                        self.remove_mssat_file(source_file)
                        # content of non-schema files is only in new files
                        self.saved_files[file_id] = deepcopy(self.new_files[file_id])
                        self.add_to_pp_files(self.saved_files[file_id])
            elif unique_id in self.saved_manifest.macros:
                macro = self.saved_manifest.macros[unique_id]
                file_id = macro.file_id
                if file_id in self.saved_files and file_id not in self.file_diff["deleted"]:
                    source_file = self.saved_files[file_id]
                    self.delete_macro_file(source_file)
                    self.saved_files[file_id] = deepcopy(self.new_files[file_id])
                    self.add_to_pp_files(self.saved_files[file_id])

    def delete_doc_node(self, source_file):
        # remove the nodes in the 'docs' dictionary
        docs = source_file.docs.copy()
        for unique_id in docs:
            self.deleted_manifest.docs[unique_id] = self.saved_manifest.docs.pop(unique_id)
            source_file.docs.remove(unique_id)
        # The unique_id of objects that contain a doc call are stored in the
        # doc source_file.nodes
        self.schedule_nodes_for_parsing(source_file.nodes)
        source_file.nodes = []
        # Remove the file object
        self.deleted_manifest.files[source_file.file_id] = self.saved_manifest.files.pop(
            source_file.file_id
        )

    # Schema files -----------------------
    # Changed schema files
    def change_schema_file(self, file_id):
        saved_schema_file = self.saved_files[file_id]
        new_schema_file = deepcopy(self.new_files[file_id])
        saved_yaml_dict = saved_schema_file.dict_from_yaml
        new_yaml_dict = new_schema_file.dict_from_yaml
        if "version" in new_yaml_dict:
            # despite the fact that this goes in the saved_schema_file, it
            # should represent the new yaml dictionary, and should produce
            # an error if the updated yaml file doesn't have a version
            saved_schema_file.pp_dict = {"version": new_yaml_dict["version"]}
        else:
            saved_schema_file.pp_dict = {}
        self.handle_schema_file_changes(saved_schema_file, saved_yaml_dict, new_yaml_dict)

        # copy from new schema_file to saved_schema_file to preserve references
        # that weren't removed
        saved_schema_file.contents = new_schema_file.contents
        saved_schema_file.checksum = new_schema_file.checksum
        saved_schema_file.dfy = new_schema_file.dfy
        # schedule parsing
        self.add_to_pp_files(saved_schema_file)
        # schema_file pp_dict should have been generated already
        fire_event(PartialParsingUpdateSchemaFile(file_id=file_id))

    # Delete schema files -- a variation on change_schema_file
    def delete_schema_file(self, file_id):
        saved_schema_file = self.saved_files[file_id]
        saved_yaml_dict = saved_schema_file.dict_from_yaml
        new_yaml_dict = {}
        self.handle_schema_file_changes(saved_schema_file, saved_yaml_dict, new_yaml_dict)
        self.deleted_manifest.files[file_id] = self.saved_manifest.files.pop(file_id)

    # For each key in a schema file dictionary, process the changed, deleted, and added
    # elemnts for the key lists
    def handle_schema_file_changes(self, schema_file, saved_yaml_dict, new_yaml_dict):
        # loop through comparing previous dict_from_yaml with current dict_from_yaml
        # Need to do the deleted/added/changed thing, just like the files lists

        env_var_changes = {}
        if schema_file.file_id in self.env_vars_changed_schema_files:
            env_var_changes = self.env_vars_changed_schema_files[schema_file.file_id]

        # models, seeds, snapshots, analyses
        for dict_key in ["models", "seeds", "snapshots", "analyses"]:
            key_diff = self.get_diff_for(dict_key, saved_yaml_dict, new_yaml_dict)
            if key_diff["changed"]:
                for elem in key_diff["changed"]:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem)
                    self.merge_patch(schema_file, dict_key, elem)
            if key_diff["deleted"]:
                for elem in key_diff["deleted"]:
                    self.delete_schema_mssa_links(schema_file, dict_key, elem)
            if key_diff["added"]:
                for elem in key_diff["added"]:
                    self.merge_patch(schema_file, dict_key, elem)
            # Handle schema file updates due to env_var changes
            if dict_key in env_var_changes and dict_key in new_yaml_dict:
                for name in env_var_changes[dict_key]:
                    if name in key_diff["changed_or_deleted_names"]:
                        continue
                    elem = self.get_schema_element(new_yaml_dict[dict_key], name)
                    if elem:
                        self.delete_schema_mssa_links(schema_file, dict_key, elem)
                        self.merge_patch(schema_file, dict_key, elem)

        # sources
        dict_key = "sources"
        source_diff = self.get_diff_for(dict_key, saved_yaml_dict, new_yaml_dict)
        if source_diff["changed"]:
            for source in source_diff["changed"]:
                if "overrides" in source:  # This is a source patch; need to re-parse orig source
                    self.remove_source_override_target(source)
                self.delete_schema_source(schema_file, source)
                self.remove_tests(schema_file, dict_key, source["name"])
                self.merge_patch(schema_file, dict_key, source)
        if source_diff["deleted"]:
            for source in source_diff["deleted"]:
                if "overrides" in source:  # This is a source patch; need to re-parse orig source
                    self.remove_source_override_target(source)
                self.delete_schema_source(schema_file, source)
                self.remove_tests(schema_file, dict_key, source["name"])
        if source_diff["added"]:
            for source in source_diff["added"]:
                if "overrides" in source:  # This is a source patch; need to re-parse orig source
                    self.remove_source_override_target(source)
                self.merge_patch(schema_file, dict_key, source)
        # Handle schema file updates due to env_var changes
        if dict_key in env_var_changes and dict_key in new_yaml_dict:
            for name in env_var_changes[dict_key]:
                if name in source_diff["changed_or_deleted_names"]:
                    continue
                source = self.get_schema_element(new_yaml_dict[dict_key], name)
                if source:
                    if "overrides" in source:
                        self.remove_source_override_target(source)
                    self.delete_schema_source(schema_file, source)
                    self.remove_tests(schema_file, dict_key, source["name"])
                    self.merge_patch(schema_file, dict_key, source)

        # macros
        dict_key = "macros"
        macro_diff = self.get_diff_for(dict_key, saved_yaml_dict, new_yaml_dict)
        if macro_diff["changed"]:
            for macro in macro_diff["changed"]:
                self.delete_schema_macro_patch(schema_file, macro)
                self.merge_patch(schema_file, dict_key, macro)
        if macro_diff["deleted"]:
            for macro in macro_diff["deleted"]:
                self.delete_schema_macro_patch(schema_file, macro)
        if macro_diff["added"]:
            for macro in macro_diff["added"]:
                self.merge_patch(schema_file, dict_key, macro)
        # Handle schema file updates due to env_var changes
        if dict_key in env_var_changes and dict_key in new_yaml_dict:
            for name in env_var_changes[dict_key]:
                if name in macro_diff["changed_or_deleted_names"]:
                    continue
                elem = self.get_schema_element(new_yaml_dict[dict_key], name)
                if elem:
                    self.delete_schema_macro_patch(schema_file, elem)
                    self.merge_patch(schema_file, dict_key, elem)

        # exposures
        dict_key = "exposures"
        exposure_diff = self.get_diff_for(dict_key, saved_yaml_dict, new_yaml_dict)
        if exposure_diff["changed"]:
            for exposure in exposure_diff["changed"]:
                self.delete_schema_exposure(schema_file, exposure)
                self.merge_patch(schema_file, dict_key, exposure)
        if exposure_diff["deleted"]:
            for exposure in exposure_diff["deleted"]:
                self.delete_schema_exposure(schema_file, exposure)
        if exposure_diff["added"]:
            for exposure in exposure_diff["added"]:
                self.merge_patch(schema_file, dict_key, exposure)
        # Handle schema file updates due to env_var changes
        if dict_key in env_var_changes and dict_key in new_yaml_dict:
            for name in env_var_changes[dict_key]:
                if name in exposure_diff["changed_or_deleted_names"]:
                    continue
                elem = self.get_schema_element(new_yaml_dict[dict_key], name)
                if elem:
                    self.delete_schema_exposure(schema_file, elem)
                    self.merge_patch(schema_file, dict_key, elem)

        # metrics
        dict_key = "metrics"
        metric_diff = self.get_diff_for("metrics", saved_yaml_dict, new_yaml_dict)
        if metric_diff["changed"]:
            for metric in metric_diff["changed"]:
                self.delete_schema_metric(schema_file, metric)
                self.merge_patch(schema_file, dict_key, metric)
        if metric_diff["deleted"]:
            for metric in metric_diff["deleted"]:
                self.delete_schema_metric(schema_file, metric)
        if metric_diff["added"]:
            for metric in metric_diff["added"]:
                self.merge_patch(schema_file, dict_key, metric)
        # Handle schema file updates due to env_var changes
        if dict_key in env_var_changes and dict_key in new_yaml_dict:
            for name in env_var_changes[dict_key]:
                if name in metric_diff["changed_or_deleted_names"]:
                    continue
                elem = self.get_schema_element(new_yaml_dict[dict_key], name)
                if elem:
                    self.delete_schema_metric(schema_file, elem)
                    self.merge_patch(schema_file, dict_key, elem)

    # Take a "section" of the schema file yaml dictionary from saved and new schema files
    # and determine which parts have changed
    def get_diff_for(self, key, saved_yaml_dict, new_yaml_dict):
        if key in saved_yaml_dict or key in new_yaml_dict:
            saved_elements = saved_yaml_dict[key] if key in saved_yaml_dict else []
            new_elements = new_yaml_dict[key] if key in new_yaml_dict else []
        else:
            return {"deleted": [], "added": [], "changed": []}
        # for each set of keys, need to create a dictionary of names pointing to entry
        saved_elements_by_name = {}
        new_elements_by_name = {}
        # sources have two part names?
        for element in saved_elements:
            saved_elements_by_name[element["name"]] = element
        for element in new_elements:
            new_elements_by_name[element["name"]] = element

        # now determine which elements, by name, are added, deleted or changed
        saved_element_names = set(saved_elements_by_name.keys())
        new_element_names = set(new_elements_by_name.keys())
        deleted = saved_element_names.difference(new_element_names)
        added = new_element_names.difference(saved_element_names)
        common = saved_element_names.intersection(new_element_names)
        changed = []
        for element_name in common:
            if saved_elements_by_name[element_name] != new_elements_by_name[element_name]:
                changed.append(element_name)

        # make lists of yaml elements to return as diffs
        deleted_elements = [saved_elements_by_name[name].copy() for name in deleted]
        added_elements = [new_elements_by_name[name].copy() for name in added]
        changed_elements = [new_elements_by_name[name].copy() for name in changed]

        diff = {
            "deleted": deleted_elements,
            "added": added_elements,
            "changed": changed_elements,
            "changed_or_deleted_names": list(changed) + list(deleted),
        }
        return diff

    # Merge a patch file into the pp_dict in a schema file
    def merge_patch(self, schema_file, key, patch):
        if not schema_file.pp_dict:
            schema_file.pp_dict = {"version": schema_file.dict_from_yaml["version"]}
        pp_dict = schema_file.pp_dict
        if key not in pp_dict:
            pp_dict[key] = [patch]
        else:
            # check that this patch hasn't already been saved
            found = False
            for elem in pp_dict[key]:
                if elem["name"] == patch["name"]:
                    found = True
            if not found:
                pp_dict[key].append(patch)
        schema_file.delete_from_env_vars(key, patch["name"])
        self.add_to_pp_files(schema_file)

    # For model, seed, snapshot, analysis schema dictionary keys,
    # delete the patches and tests from the patch
    def delete_schema_mssa_links(self, schema_file, dict_key, elem):
        # find elem node unique_id in node_patches
        prefix = key_to_prefix[dict_key]
        elem_unique_id = ""
        for unique_id in schema_file.node_patches:
            if not unique_id.startswith(prefix):
                continue
            parts = unique_id.split(".")
            elem_name = parts[-1]
            if elem_name == elem["name"]:
                elem_unique_id = unique_id
                break

        # remove elem node and remove unique_id from node_patches
        if elem_unique_id:
            # might have been already removed
            if elem_unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes.pop(elem_unique_id)
                self.deleted_manifest.nodes[elem_unique_id] = node
                # need to add the node source_file to pp_files
                file_id = node.file_id
                # need to copy new file to saved files in order to get content
                if file_id in self.new_files:
                    self.saved_files[file_id] = deepcopy(self.new_files[file_id])
                if self.saved_files[file_id]:
                    source_file = self.saved_files[file_id]
                    self.add_to_pp_files(source_file)
            # remove from patches
            schema_file.node_patches.remove(elem_unique_id)

        # for models, seeds, snapshots (not analyses)
        if dict_key in ["models", "seeds", "snapshots"]:
            # find related tests and remove them
            self.remove_tests(schema_file, dict_key, elem["name"])

    def remove_tests(self, schema_file, dict_key, name):
        tests = schema_file.get_tests(dict_key, name)
        for test_unique_id in tests:
            if test_unique_id in self.saved_manifest.nodes:
                node = self.saved_manifest.nodes.pop(test_unique_id)
                self.deleted_manifest.nodes[test_unique_id] = node
        schema_file.remove_tests(dict_key, name)

    def delete_schema_source(self, schema_file, source_dict):
        # both patches, tests, and source nodes
        source_name = source_dict["name"]
        # There may be multiple sources for each source dict, since
        # there will be a separate source node for each table.
        # ParsedSourceDefinition name = table name, dict name is source_name
        sources = schema_file.sources.copy()
        for unique_id in sources:
            if unique_id in self.saved_manifest.sources:
                source = self.saved_manifest.sources[unique_id]
                if source.source_name == source_name:
                    source = self.saved_manifest.sources.pop(unique_id)
                    self.deleted_manifest.sources[unique_id] = source
                    schema_file.sources.remove(unique_id)
                    self.schedule_referencing_nodes_for_parsing(unique_id)
                    fire_event(PartialParsingDeletedSource(unique_id=unique_id))

    def delete_schema_macro_patch(self, schema_file, macro):
        # This is just macro patches that need to be reapplied
        macro_unique_id = None
        if macro["name"] in schema_file.macro_patches:
            macro_unique_id = schema_file.macro_patches[macro["name"]]
            del schema_file.macro_patches[macro["name"]]
        if macro_unique_id and macro_unique_id in self.saved_manifest.macros:
            macro = self.saved_manifest.macros.pop(macro_unique_id)
            self.deleted_manifest.macros[macro_unique_id] = macro
            macro_file_id = macro.file_id
            if macro_file_id in self.new_files:
                self.saved_files[macro_file_id] = deepcopy(self.new_files[macro_file_id])
                self.add_to_pp_files(self.saved_files[macro_file_id])

    # exposures are created only from schema files, so just delete
    # the exposure.
    def delete_schema_exposure(self, schema_file, exposure_dict):
        exposure_name = exposure_dict["name"]
        exposures = schema_file.exposures.copy()
        for unique_id in exposures:
            exposure = self.saved_manifest.exposures[unique_id]
            if unique_id in self.saved_manifest.exposures:
                if exposure.name == exposure_name:
                    self.deleted_manifest.exposures[unique_id] = self.saved_manifest.exposures.pop(
                        unique_id
                    )
                    schema_file.exposures.remove(unique_id)
                    fire_event(PartialParsingDeletedExposure(unique_id=unique_id))

    # metric are created only from schema files, so just delete
    # the metric.
    def delete_schema_metric(self, schema_file, metric_dict):
        metric_name = metric_dict["name"]
        metrics = schema_file.metrics.copy()
        for unique_id in metrics:
            metric = self.saved_manifest.metrics[unique_id]
            if unique_id in self.saved_manifest.metrics:
                if metric.name == metric_name:
                    self.deleted_manifest.metrics[unique_id] = self.saved_manifest.metrics.pop(
                        unique_id
                    )
                    schema_file.metrics.remove(unique_id)
                    fire_event(PartialParsingDeletedMetric(id=unique_id))

    def get_schema_element(self, elem_list, elem_name):
        for element in elem_list:
            if "name" in element and element["name"] == elem_name:
                return element
        return None

    def get_schema_file_for_source(self, package_name, source_name):
        schema_file = None
        for source in self.saved_manifest.sources.values():
            if source.package_name == package_name and source.source_name == source_name:
                file_id = source.file_id
                if file_id in self.saved_files:
                    schema_file = self.saved_files[file_id]
                break
        return schema_file

    def get_source_override_file_and_dict(self, source):
        package = source["overrides"]
        source_name = source["name"]
        orig_source_schema_file = self.get_schema_file_for_source(package, source_name)
        orig_sources = orig_source_schema_file.dict_from_yaml["sources"]
        orig_source = self.get_schema_element(orig_sources, source_name)
        return (orig_source_schema_file, orig_source)

    def remove_source_override_target(self, source_dict):
        (orig_file, orig_source) = self.get_source_override_file_and_dict(source_dict)
        if orig_source:
            self.delete_schema_source(orig_file, orig_source)
            self.remove_tests(orig_file, "sources", orig_source["name"])
            self.merge_patch(orig_file, "sources", orig_source)
            self.add_to_pp_files(orig_file)

    # This builds a dictionary of files that need to be scheduled for parsing
    # because the env var has changed.
    # source_files
    #   env_vars_changed_source_files: [file_id, file_id...]
    # schema_files
    #   env_vars_changed_schema_files: {file_id: {"yaml_key": [name, ..]}}
    def build_env_vars_to_files(self):
        unchanged_vars = []
        changed_vars = []
        delete_vars = []
        # Check whether the env_var has changed and add it to
        # an unchanged or changed list
        for env_var in self.saved_manifest.env_vars:
            prev_value = self.saved_manifest.env_vars[env_var]
            current_value = os.getenv(env_var)
            if current_value is None:
                # env_var no longer set, remove from manifest
                delete_vars.append(env_var)
            if prev_value == current_value:
                unchanged_vars.append(env_var)
            else:  # prev_value != current_value
                changed_vars.append(env_var)
        for env_var in delete_vars:
            del self.saved_manifest.env_vars[env_var]

        env_vars_changed_source_files = []
        env_vars_changed_schema_files = {}
        # The SourceFiles contain a list of env_vars that were used in the file.
        # The SchemaSourceFiles contain a dictionary of yaml_key to schema entry names to
        # a list of vars.
        # Create a list of file_ids for source_files that need to be reparsed, and
        # a dictionary of file_ids to yaml_keys to names.
        for source_file in self.saved_files.values():
            file_id = source_file.file_id
            if not source_file.env_vars:
                continue
            if source_file.parse_file_type == ParseFileType.Schema:
                for yaml_key in source_file.env_vars.keys():
                    for name in source_file.env_vars[yaml_key].keys():
                        for env_var in source_file.env_vars[yaml_key][name]:
                            if env_var in changed_vars:
                                if file_id not in env_vars_changed_schema_files:
                                    env_vars_changed_schema_files[file_id] = {}
                                if yaml_key not in env_vars_changed_schema_files[file_id]:
                                    env_vars_changed_schema_files[file_id][yaml_key] = []
                                if name not in env_vars_changed_schema_files[file_id][yaml_key]:
                                    env_vars_changed_schema_files[file_id][yaml_key].append(name)
                                break  # if one env_var is changed we can stop

            else:
                for env_var in source_file.env_vars:
                    if env_var in changed_vars:
                        env_vars_changed_source_files.append(file_id)
                        break  # if one env_var is changed we can stop

        return (env_vars_changed_source_files, env_vars_changed_schema_files)
