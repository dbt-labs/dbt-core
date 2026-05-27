# Implementation Summary: Source Column Descriptions from Database

## GitHub Issue
[#10476](https://github.com/dbt-labs/dbt-core/issues/10476) - `[Feature] docs generate - retrieve column descriptions from DB when available - for Sources`

## Problem Statement
Previously, dbt required manual maintenance of column descriptions in YAML files for source tables. If database columns had comments/descriptions but weren't documented in YAML, those database comments would not appear in the generated documentation. This created unnecessary duplication of effort, especially in organizations where multiple teams manage different data sources.

## Solution Overview
This implementation enhances `dbt docs generate` to automatically use database column comments as fallback descriptions when YAML documentation is unavailable for source columns.

### Priority Order
1. **YAML description** (if present) - Takes priority, allowing users to override database comments
2. **Database comment** (if YAML is empty) - Used as fallback

## Technical Implementation

### Files Modified

#### 1. `core/dbt/task/docs/generate.py`

**New Method Added:**
```python
def _enrich_source_columns_with_descriptions(
    self, sources: Dict[str, CatalogTable]
) -> Dict[str, CatalogTable]:
```

This method:
- Iterates through all source tables in the catalog
- For each source, retrieves the corresponding YAML definition from the manifest
- For each column in the catalog:
  - If the column has a YAML description, it replaces the DB comment with the YAML description (priority)
  - If no YAML description exists, the DB comment remains (fallback)

**Integration Point:**
- Called in `GenerateTask.run()` after `catalog.make_unique_id_map()` (line 306)
- Enriches the sources dictionary before creating the final catalog artifact

**Key Design Decisions:**
- Uses the existing `comment` field in `ColumnMetadata` to store the merged description
- Avoids schema changes to `ColumnMetadata` or `CatalogTable` classes
- Uses Python's `dataclasses.replace()` to create updated column metadata immutably
- Only affects source columns, not model/seed columns

#### 2. `tests/functional/docs/test_generate.py`

**New Test Class Added:**
```python
class TestGenerateSourceColumnDescriptions(TestBaseGenerate):
```

This test:
- Creates a source table with column comments in the database
- Defines the source in YAML with partial documentation:
  - Some columns with YAML descriptions
  - Some columns without YAML descriptions
  - Some columns not in YAML at all
- Runs `dbt docs generate`
- Verifies:
  - ✅ YAML descriptions take priority over DB comments
  - ✅ DB comments are used when YAML is empty
  - ✅ DB comments are used for undocumented columns

## How It Works

### Before This Change
```
Database:                 YAML:                    Catalog Output:
-----------              ---------                 ----------------
col1 (DB comment)   ->   col1: ""            ->   col1: null
col2 (DB comment)   ->   (not in YAML)       ->   col2: null
```

### After This Change
```
Database:                 YAML:                    Catalog Output:
-----------              ---------                 ----------------
col1 (DB comment)   ->   col1: "YAML desc"   ->   col1: "YAML desc" ✅
col2 (DB comment)   ->   col2: ""            ->   col2: "DB comment" ✅
col3 (DB comment)   ->   (not in YAML)       ->   col3: "DB comment" ✅
```

## Benefits

1. **Reduced Maintenance**: No need to duplicate database comments in YAML files
2. **Automatic Documentation**: Source tables with database comments are automatically documented
3. **Override Capability**: Users can still override database comments with YAML descriptions when needed
4. **No Breaking Changes**: Existing behavior is preserved; this is purely additive
5. **Cross-Team Collaboration**: Data engineering teams can document columns at the database level, and analytics teams automatically see those descriptions in dbt docs

## Use Cases

### Use Case 1: BigQuery with Multiple Teams
A data engineering team manages BigQuery tables with column descriptions. The analytics team uses dbt to model this data. Without manual YAML documentation, the analytics team now sees the data engineering team's column descriptions in dbt docs.

### Use Case 2: Gradual Documentation
Teams can start using dbt sources immediately with database comments, then gradually add YAML descriptions where more context is needed, without losing the database comments for undocumented columns.

### Use Case 3: External Data Sources
When working with external data sources (vendor APIs, third-party databases), database comments from the source system are automatically available in dbt docs.

## Testing

### Unit Tests
- Existing unit tests pass (no changes to core catalog generation logic)

### Integration Tests
- New test class `TestGenerateSourceColumnDescriptions` validates the enrichment behavior
- Tests cover all scenarios:
  - YAML description priority
  - DB comment fallback
  - Columns not in YAML

### Manual Testing
To test manually:
1. Create a source table with column comments
2. Define the source in YAML with partial documentation
3. Run `dbt docs generate`
4. Open `target/catalog.json` and verify source column comments

## Future Enhancements

Potential future improvements:
1. Add a configuration option to control priority (DB comment vs YAML description)
2. Support merging both (e.g., "YAML: {yaml_desc} | DB: {db_comment}")
3. Add adapter-specific comment extraction for better support across databases
4. Extend to models (not just sources) if requested

## Compatibility

- **dbt Core Version**: 1.x+
- **Adapters**: Works with all adapters that support column comments (PostgreSQL, BigQuery, Snowflake, Redshift, etc.)
- **Breaking Changes**: None
- **Schema Changes**: None (uses existing `comment` field)

## References

- GitHub Issue: https://github.com/dbt-labs/dbt-core/issues/10476
- Related Discussion: https://github.com/dbt-labs/dbt-core/issues/10476#issuecomment-2526708584
