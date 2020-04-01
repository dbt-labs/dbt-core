#!/usr/bin/env python
from dataclasses import dataclass
from typing import Dict, Any
import json

from hologram import JsonSchemaMixin
from dbt.contracts.graph.manifest import WritableManifest
from dbt.contracts.results import (
    CatalogResults, ExecutionResult, FreshnessExecutionResult
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
        catalog=CatalogResults.json_schema(),
        run_results=ExecutionResult.json_schema(),
        freshness_results=FreshnessExecutionResult.json_schema(),
    )
    print(json.dumps(schemas.to_dict()))


if __name__ == '__main__':
    main()
