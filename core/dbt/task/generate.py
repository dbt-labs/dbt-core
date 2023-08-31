import os
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
import agate

from dbt.dataclass_schema import ValidationError

from .compile import CompileTask

from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.nodes import ResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import (
    NodeStatus,
    TableMetadata,
    CatalogTable,
    CatalogResults,
    PrimitiveDict,
    CatalogKey,
    StatsItem,
    StatsDict,
    ColumnMetadata,
    CatalogArtifact,
)
from dbt.exceptions import DbtInternalError, AmbiguousCatalogMatchError
from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from dbt.events.functions import fire_event
from dbt.events.types import (
    WriteCatalogFailure,
    CatalogWritten,
    CannotGenerateDocs,
    BuildingCatalog,
)
from dbt.parser.manifest import write_manifest
import dbt.utils
import dbt.compilation
import dbt.exceptions


CATALOG_FILENAME = "catalog.json"


def get_stripped_prefix(source: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """Go through the source, extracting every key/value pair where the key starts
    with the given prefix.
    """
    cut = len(prefix)
    return {k[cut:]: v for k, v in source.items() if k.startswith(prefix)}


def build_catalog_table(data) -> CatalogTable:
    # build the new table's metadata + stats
    metadata = TableMetadata.from_dict(get_stripped_prefix(data, "table_"))
    stats = format_stats(get_stripped_prefix(data, "stats:"))

    return CatalogTable(
        metadata=metadata,
        stats=stats,
        columns={},
    )


# keys are database name, schema name, table name
class Catalog(Dict[CatalogKey, CatalogTable]):
    def __init__(self, columns: List[PrimitiveDict]) -> None:
        super().__init__()
        for col in columns:
            self.add_column(col)

    def get_table(self, data: PrimitiveDict) -> CatalogTable:
        database = data.get("table_database")
        if database is None:
            dkey: Optional[str] = None
        else:
            dkey = str(database)

        try:
            key = CatalogKey(
                dkey,
                str(data["table_schema"]),
                str(data["table_name"]),
            )
        except KeyError as exc:
            raise dbt.exceptions.CompilationError(
                "Catalog information missing required key {} (got {})".format(exc, data)
            )
        table: CatalogTable
        if key in self:
            table = self[key]
        else:
            table = build_catalog_table(data)
            self[key] = table
        return table

    def add_column(self, data: PrimitiveDict):
        table = self.get_table(data)
        column_data = get_stripped_prefix(data, "column_")
        # the index should really never be that big so it's ok to end up
        # serializing this to JSON (2^53 is the max safe value there)
        column_data["index"] = int(column_data["index"])

        column = ColumnMetadata.from_dict(column_data)
        table.columns[column.name] = column

    def make_unique_id_map(
        self, manifest: Manifest
    ) -> Tuple[Dict[str, CatalogTable], Dict[str, CatalogTable]]:
        nodes: Dict[str, CatalogTable] = {}
        sources: Dict[str, CatalogTable] = {}

        node_map, source_map = get_unique_id_mapping(manifest)
        table: CatalogTable
        for table in self.values():
            key = table.key()
            if key in node_map:
                unique_id = node_map[key]
                nodes[unique_id] = table.replace(unique_id=unique_id)

            unique_ids = source_map.get(table.key(), set())
            for unique_id in unique_ids:
                if unique_id in sources:
                    raise AmbiguousCatalogMatchError(
                        unique_id,
                        sources[unique_id].to_dict(omit_none=True),
                        table.to_dict(omit_none=True),
                    )
                else:
                    sources[unique_id] = table.replace(unique_id=unique_id)
        return nodes, sources


def format_stats(stats: PrimitiveDict) -> StatsDict:
    """Given a dictionary following this layout:

        {
            'encoded:label': 'Encoded',
            'encoded:value': 'Yes',
            'encoded:description': 'Indicates if the column is encoded',
            'encoded:include': True,

            'size:label': 'Size',
            'size:value': 128,
            'size:description': 'Size of the table in MB',
            'size:include': True,
        }

    format_stats will convert the dict into a StatsDict with keys of 'encoded'
    and 'size'.
    """
    stats_collector: StatsDict = {}

    base_keys = {k.split(":")[0] for k in stats}
    for key in base_keys:
        dct: PrimitiveDict = {"id": key}
        for subkey in ("label", "value", "description", "include"):
            dct[subkey] = stats["{}:{}".format(key, subkey)]

        try:
            stats_item = StatsItem.from_dict(dct)
        except ValidationError:
            continue
        if stats_item.include:
            stats_collector[key] = stats_item

    # we always have a 'has_stats' field, it's never included
    has_stats = StatsItem(
        id="has_stats",
        label="Has Stats?",
        value=len(stats_collector) > 0,
        description="Indicates whether there are statistics for this table",
        include=False,
    )
    stats_collector["has_stats"] = has_stats
    return stats_collector


def mapping_key(node: ResultNode) -> CatalogKey:
    dkey = dbt.utils.lowercase(node.database)
    return CatalogKey(dkey, node.schema.lower(), node.identifier.lower())


def get_unique_id_mapping(
    manifest: Manifest,
) -> Tuple[Dict[CatalogKey, str], Dict[CatalogKey, Set[str]]]:
    # A single relation could have multiple unique IDs pointing to it if a
    # source were also a node.
    node_map: Dict[CatalogKey, str] = {}
    source_map: Dict[CatalogKey, Set[str]] = {}
    for unique_id, node in manifest.nodes.items():
        key = mapping_key(node)
        node_map[key] = unique_id

    for unique_id, source in manifest.sources.items():
        key = mapping_key(source)
        if key not in source_map:
            source_map[key] = set()
        source_map[key].add(unique_id)
    return node_map, source_map


class GenerateTask(CompileTask):
    def run(self) -> CatalogArtifact:
        compile_results = None
        if self.args.compile:
            compile_results = CompileTask.run(self)
            if any(r.status == NodeStatus.Error for r in compile_results):
                fire_event(CannotGenerateDocs())
                return CatalogArtifact.from_results(
                    nodes={},
                    sources={},
                    generated_at=datetime.utcnow(),
                    errors=None,
                    compile_results=compile_results,
                )

        shutil.copyfile(
            DOCS_INDEX_FILE_PATH, os.path.join(self.config.project_target_path, "index.html")
        )

        for asset_path in self.config.asset_paths:
            to_asset_path = os.path.join(self.config.project_target_path, asset_path)

            if os.path.exists(to_asset_path):
                shutil.rmtree(to_asset_path)

            if os.path.exists(asset_path):
                shutil.copytree(asset_path, to_asset_path)

        if self.manifest is None:
            raise DbtInternalError("self.manifest was None in run!")

        if self.args.empty_catalog:
            catalog_table: agate.Table = agate.Table([])
            exceptions: List[Exception] = []
        else:
            adapter = get_adapter(self.config)
            with adapter.connection_named("generate_catalog"):
                fire_event(BuildingCatalog())
                catalog_table, exceptions = adapter.get_catalog(self.manifest)

        catalog_data: List[PrimitiveDict] = [
            dict(zip(catalog_table.column_names, map(dbt.utils._coerce_decimal, row)))
            for row in catalog_table
        ]

        catalog = Catalog(catalog_data)

        errors: Optional[List[str]] = None
        if exceptions:
            errors = [str(e) for e in exceptions]

        nodes, sources = catalog.make_unique_id_map(self.manifest)
        results = self.get_catalog_results(
            nodes=nodes,
            sources=sources,
            generated_at=datetime.utcnow(),
            compile_results=compile_results,
            errors=errors,
        )

        path = os.path.join(self.config.project_target_path, CATALOG_FILENAME)
        results.write(path)
        if self.args.compile:
            write_manifest(self.manifest, self.config.project_target_path)

        if exceptions:
            fire_event(WriteCatalogFailure(num_exceptions=len(exceptions)))
        fire_event(CatalogWritten(path=os.path.abspath(path)))
        return results

    def get_catalog_results(
        self,
        nodes: Dict[str, CatalogTable],
        sources: Dict[str, CatalogTable],
        generated_at: datetime,
        compile_results: Optional[Any],
        errors: Optional[List[str]],
    ) -> CatalogArtifact:
        return CatalogArtifact.from_results(
            generated_at=generated_at,
            nodes=nodes,
            sources=sources,
            compile_results=compile_results,
            errors=errors,
        )

    @classmethod
    def interpret_results(self, results: Optional[CatalogResults]) -> bool:
        if results is None:
            return False
        if results.errors:
            return False
        compile_results = results._compile_results
        if compile_results is None:
            return True

        return super().interpret_results(compile_results)
