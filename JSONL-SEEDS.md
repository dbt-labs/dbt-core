# JSONL Object Seeds for dbt-core

Design for dbt-core issue #2365: support newline-delimited JSON seed files
alongside CSV seeds.

## Goal

Add support for seed files with extensions `.jsonl` and `.ndjson`. Each
non-empty line must be a valid JSON **object**. The loader converts top-level
object keys into agate columns and values into agate cell values, so the
existing seed execution path continues to receive an `agate.Table` unchanged.

## Non-goals

- Top-level JSON arrays.
- Multi-line JSON records.
- Adapter-native bulk JSONL loading (BigQuery, etc.).
- Schema inference for nested fields.
- Flattening nested objects into dotted columns.
- Streaming huge files with constant memory. Seeds are for small,
  version-controlled datasets.

## User-facing behavior

Given `seeds/users.jsonl`:

```jsonl
{"id": 1, "name": "alpha", "metadata": {"source": "manual", "rank": 1}}
{"id": 2, "name": "beta", "metadata": {"source": "api", "flags": ["x", "y"]}, "active": true}
```

dbt builds an agate table equivalent to:

```
id | name  | metadata                                  | active
1  | alpha | {"rank":1,"source":"manual"}              | null
2  | beta  | {"flags":["x","y"],"source":"api"}        | true
```

Top-level keys become columns. Missing keys become `null`. Nested objects and
arrays are preserved as compact canonical JSON strings in the cell.

Users can set `column_types` to map JSON columns to native warehouse types:

```yaml
seeds:
  my_project:
    users:
      +column_types:
        metadata: json
```

## File discovery

`core/dbt/parser/read_files.py` — `get_file_types_for_project()` currently
maps `ParseFileType.Seed` to extension `.csv` only. Add `.jsonl` and
`.ndjson`:

```python
ParseFileType.Seed: {
    "paths": project.seed_paths,
    "extensions": [".csv", ".jsonl", ".ndjson"],
    "parser": "SeedParser",
}
```

The `read_files_for_parser()` function already iterates over extensions, so
this is sufficient for both full-parse and partial-parse (`ReadFilesFromDiff`)
paths. The diff path builds its extension lookup from the same
`get_file_types_for_project()` return value, so `.jsonl`/`.ndjson` will map
to `ParseFileType.Seed` automatically.

`load_seed_source_file()` already reads file contents for checksum and sets
`contents = ""` for non-large seeds — same behavior for JSONL.

Node naming uses `os.path.splitext`, so `users.jsonl` produces node name
`users`. Having both `users.csv` and `users.jsonl` in the same directory will
produce a naming collision (same as having two CSV files with the same stem
in the same directory).

## Integration point: `load_agate_table()`

The critical integration point is `load_agate_table()` in
`core/dbt/context/providers.py:1285`. This is a `@contextmember()` on the
Jinja context, called from the seed materialization macro as
`{{ load_agate_table() }}`.

Current flow:

1. `load_agate_table()` resolves the seed file path from the node's
   `original_file_path`.
2. Reads `column_types` and `delimiter` from `self.model.config`.
3. Validates `column_types` keys against CSV header via `_read_csv_header()`.
4. Calls `agate_helper.from_csv(path, text_cols=..., delimiter=...)`.
5. Returns the `agate.Table`. The macro stores it via
   `store_result("agate_table", ...)`.

The change: after path resolution, dispatch by file extension:

```python
ext = os.path.splitext(path)[1].lower()
if ext == ".csv":
    # existing CSV path (unchanged)
    table = agate_helper.from_csv(path, text_cols=filtered_column_types, delimiter=delimiter)
elif ext in {".jsonl", ".ndjson"}:
    table = load_jsonl_seed_agate_table(path, column_types=filtered_column_types)
else:
    raise DbtInternalError(f"Unsupported seed extension: {ext}")
```

The JSONL branch skips `delimiter` (CSV-only config) and uses a different
column-type validation approach (see below).

## New module: `core/dbt/task/seed_readers.py`

The JSONL parsing and agate construction logic lives here. Execution-time
ownership is correct — parse-time code only discovers files and computes
checksums; actual data loading happens at run time.

### Public API

```python
from pathlib import Path
import agate

def load_jsonl_seed_agate_table(
    path: str,
    column_types: dict[str, str] | None = None,
) -> agate.Table:
    ...
```

### JSONL parsing rules

Valid records: each non-empty line must parse as a JSON object.

Whitespace-only lines are ignored.

Invalid records raise an error with filename and line number:

```
Invalid JSONL seed at seeds/users.jsonl:17.
Expected each non-empty line to be a JSON object; got list.
```

For JSON decode failures:

```
Invalid JSONL seed at seeds/users.jsonl:17.
Could not parse JSON: Expecting ',' delimiter.
```

Empty files (no valid JSON objects) raise:

```
JSONL seed file 'seeds/users.jsonl' is empty or contains no valid JSON objects.
```

### Column discovery

Deterministic first-seen ordering:

1. Iterate records in file order.
2. For each object, iterate keys in JSON object order.
3. Append a key to `column_names` the first time it appears.
4. Missing keys become `None`.

### Duplicate keys inside one object

Use Python's standard `json.loads` behavior: last duplicate key wins. No
duplicate-key detection in the MVP.

### Value conversion

```python
def _normalize_jsonl_value(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    raise DbtInternalError(f"Unexpected JSON value type: {type(value)}")
```

- `dict` and `list` are encoded as compact JSON strings.
- `sort_keys=True` gives stable, reproducible values.
- `ensure_ascii=False` preserves Unicode naturally.
- Top-level list records are rejected; nested arrays inside object fields are
  allowed and preserved as JSON strings.

### Agate table construction

After collecting all rows:

1. Identify columns that ever contain a nested JSON value (serialized
   `dict`/`list`). Force these to `agate.Text` so agate doesn't try to infer
   weird types from JSON-looking strings.
2. For purely scalar columns, allow agate to infer types (number, boolean,
   date, text).
3. Explicitly pass `column_types` to `agate.Table()` to avoid relying on
   agate's default inference across heterogeneous data.

### Column type validation

CSV validation reads the header row to check `column_types` keys. JSONL has
no header row. Instead, validate after constructing the table:

1. Build the agate table from the JSONL file.
2. Compare `column_types` keys against the table's column names.
3. Log a warning for any `column_types` keys that don't appear as columns
   (same warning text as CSV path).
4. Filter `column_types` to valid columns only.

## Config interaction

- `column_types`: continues to control database column type. Works for both
  CSV and JSONL seeds.
- `delimiter`: CSV-only. Ignored for JSONL seeds.
- `quote_columns`: CSV-only. Ignored for JSONL seeds.

## What is NOT changed

- Seed selection semantics.
- `ref()` behavior.
- Seed node naming (beyond the extension in `original_file_path`).
- Existing CSV parsing or materialization macros.
- `SeedParser`, `SeedConfig`, or `SeedNode` data model.

## Tests

Functional tests in `tests/functional/seeds/`:

- `.jsonl` seed is discovered.
- `.ndjson` seed is discovered.
- `dbt seed --select my_seed` loads a JSONL seed.
- Downstream model can `ref("my_seed")`.
- Missing keys become null.
- New columns discovered after row 1 are included.
- Nested object becomes compact JSON string.
- Nested array inside an object field becomes compact JSON string.
- Top-level array errors with filename and line number.
- Top-level scalar errors with filename and line number.
- Malformed JSON errors with filename and line number.
- Empty lines are ignored.
- Empty file errors with clear message.
- Existing `.csv` seed tests still pass unchanged.
- `column_types` works for at least one JSONL column.
- `column_types` for non-existent JSONL columns produces warning.

Test fixture:

```jsonl
{"id":1,"name":"alpha","metadata":{"source":"manual","rank":1}}
{"id":2,"active":true,"metadata":{"source":"api","flags":["x","y"]}}
{"id":3,"name":null}
```

Expected columns: `id, name, metadata, active`

Expected metadata values:
- `{"rank":1,"source":"manual"}`
- `{"flags":["x","y"],"source":"api"}`
- `null`

## Open questions for maintainers

1. Extension-based detection is sufficient (consistent with `.sql`/`.py` for
   models). No `+seed_format` config needed.
2. Nested arrays inside object fields: allowed, serialized as compact JSON.
3. `sort_keys=True` for deterministic nested JSON output: yes.
4. Empty files: clear error message, not agate's generic error.
5. Duplicate keys: punt, use `json.loads` default behavior.

## MVP PR shape

- Add `.jsonl` and `.ndjson` to seed file discovery.
- Add `core/dbt/task/seed_readers.py` with JSONL-to-agate loader.
- Modify `load_agate_table()` in `providers.py` to dispatch by extension.
- Preserve all existing CSV behavior.
- Serialize nested JSON to compact strings.
- Add functional tests.
- Do not add adapter-native BigQuery loading.
