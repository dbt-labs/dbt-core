from __future__ import print_function
import os
import oyaml as yaml  # NOTE: New dependency

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.utils import is_enabled as check_is_enabled
from dbt.task.generate import unflatten  # NOTE: Should we move this somewhere else?

import dbt.ui.printer
from dbt.task.base_task import BaseTask


class BootstrapTask(BaseTask):
    def _get_manifest(self):
        compiler = dbt.compilation.Compiler(self.config)
        compiler.initialize()

        all_projects = compiler.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(self.config, all_projects)
        return manifest

    def _convert_single_relation_dict_to_yml(self, relation_dict):
        to_yaml = {}
        to_yaml["version"] = 2
        to_yaml["models"] = relation_dict
        # NOTE: Do we want to increase the indentation?
        # https://stackoverflow.com/questions/25108581/python-yaml-dump-bad-indentation
        return yaml.dump(to_yaml, default_flow_style=False)

    def write_relation(self, design_file_path, relation_dict):
        if os.path.isfile(design_file_path):
            logger.info(
                dbt.ui.printer.yellow(
                    "Warning: File {} already exists. Skipping".format(design_file_path)
                )
            )
            return

        logger.info("Creating design file: {}".format(design_file_path))

        yml = self._convert_single_relation_dict_to_yml(relation_dict)
        with open(design_file_path, "w") as f:
            f.write(yml)

    def print_relation(self, relation_dict):
        yml = self._convert_single_relation_dict_to_yml(relation_dict)
        logger.info(yml)

    def prep_metadata(self, meta_dict):
        columns = []
        for colname in meta_dict["columns"]:
            column = {}
            column["name"] = colname
            columns.append(column)

        model = {}
        model["name"] = meta_dict["metadata"]["name"]
        if meta_dict["metadata"]["comment"]:
            description = meta_dict["metadata"]["comment"]
        else:
            description = "TODO: Replace me"

        model["description"] = description
        model["columns"] = columns

        return model

    def run(self):
        single_file = self.args.single_file
        print_only = self.args.print_only
        schemas = self.args.schemas

        logger.info("Bootstrapping the following schemas:")
        for schema in schemas:
            logger.info("- {}".format(schema))

        # Look up all of the relations in the DB
        manifest = self._get_manifest()
        adapter = get_adapter(self.config)
        all_relations = adapter.get_unfiltered_catalog(manifest)

        selected_relations = all_relations.where(
            lambda row: row["table_schema"] in schemas
        )

        zipped_relations = [
            dict(zip(selected_relations.column_names, row))
            for row in selected_relations
        ]

        relations_to_design = unflatten(zipped_relations)

        if len(relations_to_design) == 0:
            logger.info(
                dbt.ui.printer.yellow(
                    "Warning: No relations found in selected schemas: {}."
                    "\nAborting.".format(schemas)
                )
            )
            return {}

        for schema, relations in relations_to_design.items():
            schema_path = os.path.join("models", schema)
            if print_only:
                pass
            elif os.path.isdir(schema_path):
                logger.info(
                    dbt.ui.printer.yellow(
                        "Warning: Directory {} already exists. \n"
                        "Proceeding with caution.".format(schema_path)
                    )
                )
            else:
                os.mkdir(schema_path)

            all_models = []

            for relation, meta_data in relations.items():

                relation_dict = self.prep_metadata(meta_data)
                all_models.append(relation_dict)

                if not single_file:
                    if print_only:
                        logger.info("-" * 20)
                        logger.info(
                            "Design for relation: {}.{}".format(schema, relation)
                        )
                        logger.info("-" * 20)
                        self.print_relation([relation_dict])
                    else:
                        design_file_name = "{}.yml".format(relation)
                        design_file_path = os.path.join(schema_path, design_file_name)
                        self.write_relation(design_file_path, [relation_dict])

            if single_file:
                if print_only:
                    logger.info("-" * 20)
                    logger.info("Design for schmea: {}".format(schema))
                    logger.info("-" * 20)
                    self.print_relation(all_models)
                else:
                    design_file_name = "{}.yml".format(schema)
                    design_file_path = os.path.join(schema_path, design_file_name)
                    self.write_relation(design_file_path, all_models)

        return all_models

    def interpret_results(self, results):
        return len(results) != 0
