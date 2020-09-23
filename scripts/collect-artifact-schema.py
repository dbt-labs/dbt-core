#!/usr/bin/env python
from dataclasses import dataclass
from typing import Dict, Any
import json

from hologram import JsonSchemaMixin
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    CatalogArtifact, RunResultsArtifact, FreshnessExecutionResultArtifact
)


@dataclass
class Schemas(JsonSchemaMixin):
    manifest: Dict[str, Any]
    catalog: Dict[str, Any]
    run_results: Dict[str, Any]
    freshness_results: Dict[str, Any]


def main():
    schemas = Schemas(
        manifest=WritableManifest.json_schema(),
        catalog=CatalogArtifact.json_schema(),
        run_results=RunResultsArtifact.json_schema(),
        freshness_results=FreshnessExecutionResultArtifact.json_schema(),
    )
    print(json.dumps(schemas.to_dict()))


if __name__ == '__main__':
    main()
