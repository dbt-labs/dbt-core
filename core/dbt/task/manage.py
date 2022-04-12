# coding=utf-8
from typing import Dict, Set, Tuple

from .compile import CompileTask
from .runnable import ManifestTask
from dbt.exceptions import warn_or_error, ValidationException
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
)
from dbt.contracts.project import PruneModelsAction


class ManageTask(CompileTask):
    def run(self):
        ManifestTask._runtime_initialize(self)
        models_in_codebase = self.manifest.nodes.keys()
        adapter = get_adapter(self.config)

        with adapter.connection_named("master"):
            required_schemas = self.get_model_schemas(adapter, models_in_codebase)
            self.populate_adapter_cache(adapter, required_schemas)

            adapter.clear_transaction()
            self._prune_models(adapter)

    def _prune_models(self, adapter):
        self._assert_schema_uniqueness()

        if len(self.config.managed_schemas) == 0:
            warn_or_error("No schema's configured to manage")
            return

        models_in_codebase: Set[Tuple[str, str, str]] = set(
            (n.config.database, n.config.schema, n.config.alias)
            for n in self.manifest.nodes.values()
            if isinstance(n, ParsedModelNode)
        )

        # get default 'database' + 'schema' for active target
        creds = adapter.connections.profile.credentials
        default_database, default_schema = creds.database, creds.schema

        for config in self.config.managed_schemas:
            database = config.database or default_database
            schema = config.schema or default_schema

            models_in_database: Dict[Tuple[str, str, str], str] = {
                (database, schema, relation.identifier): relation
                for relation in adapter.list_relations(database, schema)
            }
            if len(models_in_database) == 0:
                warn_or_error(
                    f"No objects in managed schema '{database}.{schema}'"
                )

            should_act_upon = models_in_database.keys() - models_in_codebase

            for (target_database, target_schema, target_identifier) in sorted(should_act_upon):
                target_action = config.prune_models or PruneModelsAction.SKIP
                if target_action == PruneModelsAction.WARN:
                    warn_or_error(
                        f"Found unused model {target_database}.{target_schema}.{target_identifier}"
                    )
                elif target_action == PruneModelsAction.DROP:
                    adapter.drop_relation(
                        models_in_database[(target_database, target_schema, target_identifier)]
                    )

    def _assert_schema_uniqueness(self):
        schemas = set()

        for config in self.config.managed_schemas:
            schema = (config.database, config.schema)
            if schema in schemas:
                raise ValidationException(f"Duplicate schema found: {schema}")
            schemas.add(schema)

    def interpret_results(self, results):
        return True
