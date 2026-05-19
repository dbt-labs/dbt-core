# dbt-docs-server API contracts

## Contents

- [dbt-docs-server API contracts](#dbt-docs-server-api-contracts)
  - [Contents](#contents)
  - [How to use this document](#how-to-use-this-document)
  - [Conventions](#conventions)
  - [ADR-1: Single-resource detail endpoint structure](#adr-1-single-resource-detail-endpoint-structure)
    - [Options considered](#options-considered)
    - [Backend prerequisite](#backend-prerequisite)
  - [ADR-2: `execution_info` placement](#adr-2-execution_info-placement)
    - [Options considered](#options-considered-1)
  - [Backend conventions](#backend-conventions)
    - [`NodeBase` struct](#nodebase-struct)
    - [Capability flags](#capability-flags)
  - [`GET /api/v1/models/:id`](#get-apiv1modelsid)
    - [Example response](#example-response)
    - [Field reference](#field-reference)
    - [Type definition](#type-definition)
    - [Risk register](#risk-register)
  - [ADR-3: `GET /api/v1/tests/:id` response shape](#adr-3-get-apiv1testsid-response-shape-for-test-vs-unit_test)
  - [`GET /api/v1/nodes/:id` (deferred)](#get-apiv1nodesid-deferred)
  - [ADR-4: `execution_info` field naming — single-run semantics](#adr-4-execution_info-field-naming--single-run-semantics)
  - [ADR-5: `execution_info` omission for definition-only and Semantic Layer resources](#adr-5-execution_info-omission-for-definition-only-and-semantic-layer-resources)
  - [`GET /api/v1/sources/:id`](#get-apiv1sourcesid)
  - [`GET /api/v1/seeds/:id`](#get-apiv1seedsid)
  - [`GET /api/v1/snapshots/:id`](#get-apiv1snapshotsid)
  - [`GET /api/v1/tests/:id`](#get-apiv1testsid)
  - [Design notes — `GET /api/v1/exposures/:id`](#design-notes--get-apiv1exposuresid)
  - [`GET /api/v1/exposures/:id`](#get-apiv1exposuresid)
  - [Design notes — `GET /api/v1/groups/:id`](#design-notes--get-apiv1groupsid)
  - [`GET /api/v1/groups/:id`](#get-apiv1groupsid)
  - [Design notes — `GET /api/v1/macros/:id`](#design-notes--get-apiv1macrosid)
  - [`GET /api/v1/macros/:id`](#get-apiv1macrosid)
  - [Design notes — `GET /api/v1/metrics/:id`](#design-notes--get-apiv1metricsid)
  - [`GET /api/v1/metrics/:id`](#get-apiv1metricsid)
  - [Design notes — `GET /api/v1/saved_queries/:id`](#design-notes--get-apiv1saved_queriesid)
  - [`GET /api/v1/saved_queries/:id`](#get-apiv1saved_queriesid)
  - [Design notes — `GET /api/v1/semantic_models/:id`](#design-notes--get-apiv1semantic_modelsid)
  - [`GET /api/v1/semantic_models/:id`](#get-apiv1semantic_modelsid)

---

## How to use this document

All architectural decisions about the dbt-docs-server REST API are recorded here as ADRs.
When an ADR status is "Decided", the decision is **closed** — do not re-litigate it in
PRs or planning. To change a closed decision, add a new ADR that supersedes the old one.

Every new endpoint contract must be appended here before implementation begins. A PR that
implements an endpoint without a corresponding contract entry should be rejected in review.

Use `claude/prompts/dbt-docs-parity.md` (local, untracked) to run a parity analysis
that produces the next contract entry.

---

## Conventions

Field naming, data classification, and pagination follow the methodology in
`/Users/eddowh/codaz/poc-dbt-index-docs/FEATURE-TO-ENDPOINT-MAPPING.md`
(outside this repo). Key cross-cutting rules:

| Code | Rule |
|---|---|
| CC-1 | `snake_case` for all JSON field names and REST path segments |
| CC-2 | Preserve nested objects from Discovery API shape; do not flatten (exception: singleton wrappers may be flattened) |
| CC-3 | Nullable fields gated by `Capabilities` flags; no query variants |
| CC-4 | Cursor-based pagination (`?first=&after=`) for list endpoints at scale |
| CC-5 | Three classes of "internal" data: **A** = Discovery-internal but parquet-backed → promote to public REST; **B** = no parquet path → exclude; **C** = public in Discovery but CodexDB-only → stub 412 with `upgrade_path` |
| CC-6 | Inline edge arrays (`depends_on`, `referenced_by`, `models[]` member lists) accept an optional `?first=<n>` query parameter and signal truncation with `truncated: true` on the response. Default cap: 500. The flag is for client-side messaging; cursor pagination via a sub-resource is deferred until a real consumer hits the cap. Applies to every typed detail endpoint that exposes inline arrays. |
| CC-7 | JSON-string parquet columns (`meta`, `config`, `type_params`, `query_params`, `exports`, `arguments`, `agg_params`, `validity_params`, `metric_filter`, `non_additive_dimension`, etc.) are deserialized handler-side via a shared `json_parse_or_null` helper in `src/handlers/json.rs`. Failed parse → emit `null` and `tracing::warn`; never bubble the error to the client and never leak escaped JSON strings to the response. |

---

## ADR-1: Single-resource detail endpoint structure

**Status:** Decided — type-specific endpoints chosen for v0. Generic dispatcher deferred.
**Trigger to revisit:** MCP is added to dbt-docs-server.

### Options considered

**Generic endpoint only: `GET /api/v1/nodes/:id`**

Returns all resource types as a discriminated union keyed by `resource_type`.

- Pro: single endpoint; no surface proliferation; aligns with `GET /api/v1/nodes` list.
- Pro: FEATURE-TO-ENDPOINT-MAPPING.md Appendix B3 recommended this initially.
- **Con:** TypeScript union types are a UX tax — on a dedicated Model detail page the
  frontend already knows it's rendering a model; narrowing adds overhead at every call
  site on every dedicated detail page.
- **Con:** OpenAPI `oneOf` + discriminator codegen ergonomics depend on toolchain;
  `openapi-typescript` handles it, `swagger-codegen` does not. Adding a codegen pipeline
  as a prerequisite for basic productivity is the wrong tradeoff for v0.
- **Con:** Diverges from Discovery API's per-type operation structure; FE engineers
  familiar with that API face an impedance mismatch.

→ **Rejected.** Union overhead at every call site; benefit accrues only to server-side
maintainers.

---

**Type-specific endpoints: `GET /api/v1/models/:id`, `GET /api/v1/sources/:id`, etc.**

Each resource type has its own endpoint returning a standalone TypeScript type.

- Pro: `ModelDetail`, `SourceDetail`, `TestDetail` are clean standalone types.
- Pro: Mirrors Discovery API's per-type operation structure.
- Pro: FE engineers explicitly requested this.
- Pro: Each endpoint is independently testable and evolvable.
- Con: N endpoints to maintain as resource types grow.
- Con: No generic "give me any resource by `unique_id`" — but this use case does not
  exist in v0 UI: detail pages know the type from routing; lineage components know from
  `resource_type` already present in `NodeSummary`.
- Con: Common fields repeated across Rust structs unless a `NodeBase` is factored out —
  mitigated by the backend prerequisite below.

→ **Chosen for v0.**

---

**Type-specific + generic dispatcher**

Type-specific endpoints exist as above. `GET /api/v1/nodes/:id` also exists as a thin
router that parses the `unique_id` prefix (`model.`, `source.`, etc.) and delegates to
the appropriate typed handler.

- Pro: Adds back the "I have a `unique_id` and don't know the type" escape hatch —
  useful for MCP tools and AI agents.
- Pro: Additive over the chosen option; no rework required.
- Con: No identified v0 UI use case.
- Con: Unnecessary surface invites misuse and undermines the clean-type story.

→ **Deferred, not rejected.** Trigger: MCP lands in dbt-docs-server. At that point a
one-afternoon addition — provided `NodeBase` is already factored out.

### Backend prerequisite

**All typed detail handlers must compose a shared `NodeBase` Rust struct.** Without it,
adding the generic dispatcher later requires duplicating SQL queries across N handlers.
With it, the dispatcher is a string split plus a match expression.

---

## ADR-2: `execution_info` placement

**Status:** Decided — inline in each resource detail response, null-gated by capability.
**Trigger to revisit:** Run history (last N runs) becomes a product requirement.

### Options considered

**Inline in model detail response**

`execution_info` is a nested object in `ModelDetail`. `null` when `dbt build` hasn't
run (gated by `has_run_results` capability).

- Pro: One request for everything the page needs.
- Pro: Consistent with how `columns[]` is already inlined.
- Pro: Recommended by FEATURE-TO-ENDPOINT-MAPPING.md POC analysis for v0.
- Con: If run history (last N runs) is added later, the inline field forces a breaking
  schema change rather than an additive one.

**Separate sub-resource: `GET /api/v1/models/:id/run-results`**

- Pro: Run history can be added later without breaking the model detail contract.
- Con: Two round trips per page render; more client complexity.
- Con: Over-engineered for v0 where only latest run is needed.

→ **Inline chosen.** If run history is added later, promote to a sub-resource — that
change requires a deprecation period or version bump.

---

## Backend conventions

### `NodeBase` struct

All typed detail handlers compose this struct for fields shared across all resource types.

```rust
// Fields common to every resource type — all typed handlers compose this.
// Precondition for ADR-1's deferred generic dispatcher to remain cheap to add.
struct NodeBase {
    unique_id: String,
    name: String,
    resource_type: String,
    package_name: Option<String>,
    description: Option<String>,
    original_file_path: Option<String>,
    tags: Vec<String>,    // dbt.nodes — not yet queried in existing handler; add to SELECT
    fqn: Vec<String>,     // dbt.nodes — not yet queried in existing handler; add to SELECT
}
```

### Capability flags

New flags introduced by these contracts (existing: `has_column_lineage`):

| Flag | Gated surface | Parquet source | Precondition |
|---|---|---|---|
| `has_run_results` | `execution_info` on all resource detail responses | `dbt_rt.run_results` | `dbt build` ran |
| `has_catalog_stats` | `catalog.*` on model/source detail; `catalog_type` on columns | `dbt.catalog_tables` | `dbt docs generate` ran |
| `has_source_freshness` | `freshness` on source detail responses | `dbt.source_freshness` | `dbt source freshness` ran — see Risk #2 in source contract |

These flags must be added to: (1) the `Capabilities` Rust struct, (2) the
`/api/v1/capabilities` handler, (3) the TypeScript `Capabilities` interface in
`web/src/api.ts`. Implementation is tracked here; it is a separate task.

---

## `GET /api/v1/models/:id`

Powers: `ModelView` / `ResourceDetailsPage` in dbt-ui.

dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/ModelView.tsx`

GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/model.ts`

### Example response

Fields marked `// conditional` are `null` when their capability gate is absent.
Fields marked `// 🔧` are not yet returned — they require a backend change.

```json
{
  "unique_id": "model.jaffle_shop.orders",
  "name": "orders",
  "resource_type": "model",
  "package_name": "jaffle_shop",
  "materialized": "table",
  "description": "Final orders model combining payments and order status.",
  "database_name": "prod",
  "schema_name": "dbt_prod",
  "relation_name": "prod.dbt_prod.orders",
  "identifier": "orders",
  "original_file_path": "models/orders.sql",
  "file_path": "models/orders.sql",
  "access_level": "public",
  "group_name": "finance",
  "raw_code": "select order_id, ...\nfrom {{ ref('stg_orders') }}",
  "compiled_code": "select order_id, ...\nfrom prod.dbt_prod.stg_orders",
  "contract_enforced": true,
  "tags": ["finance", "core"],
  "fqn": ["jaffle_shop", "orders"],
  "columns": [
    {
      "name": "order_id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique order identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "depends_on": [
    { "unique_id": "model.jaffle_shop.stg_orders", "edge_type": "model" },
    { "unique_id": "model.jaffle_shop.stg_payments", "edge_type": "model" }
  ],
  "referenced_by": [
    { "unique_id": "exposure.jaffle_shop.revenue_dashboard", "edge_type": "exposure" }
  ],
  "execution_info": {
    "status": "success",
    "execution_time": 4.2,
    "completed_at": "2026-05-15T10:32:11Z"
  },
  "catalog": {
    "type": "table",
    "owner": "dbt_runner",
    "bytes_stat": null,
    "row_count_stat": null
  }
}
```

`execution_info` is `null` when `dbt_rt.run_results` has no rows for this model (i.e., `dbt build` has not run or produced no result for this node).
`catalog` is `null` when `dbt.catalog_tables` has no rows for this model (i.e., `dbt docs generate` has not run).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"model.pkg.name"` |
| `name` | `string` | Core | ✅ | — | |
| `resource_type` | `"model"` | Core | ✅ | — | Always `"model"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | |
| `original_file_path` | `string \| null` | Core | ✅ | — | Relative to project root |
| `file_path` | `string \| null` | Core | 🔧 | — | From `dbt.nodes.file_path`; for models equals `original_file_path` (compiled SQL is in a separate compile target, not surfaced here) |
| `tags` | `string[]` | Core | ✅ | — | `List(Utf8)` column in `dbt.nodes` |
| `fqn` | `string[]` | Core | ✅ | — | `List(Utf8)` column in `dbt.nodes` |
| `materialized` | `string \| null` | Core | ✅ | — | `"table"` · `"view"` · `"incremental"` · `"ephemeral"` |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `relation_name` | `string \| null` | Core | ✅ | — | Fully qualified: `db.schema.name` |
| `identifier` | `string \| null` | Core | ✅ | — | |
| `access_level` | `string \| null` | Core | ✅ | — | `"public"` · `"protected"` · `"private"` — see Risk #6 |
| `group_name` | `string \| null` | Core | ✅ | — | |
| `contract_enforced` | `boolean \| null` | Core | ✅ | — | |
| `raw_code` | `string \| null` | Core | ✅ | — | |
| `compiled_code` | `string \| null` | Core | ✅ | — | Confirmed present in `dbt.nodes`; matches `raw_code` for SQL models without macros |
| `columns` | `ModelColumn[]` | Core | ✅ | — | Empty array if no columns declared |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | Column order |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `null` when catalog absent | Warehouse-verified type; `null` unless `dbt docs generate` ran |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | Semantic layer use |
| `depends_on` | `EdgeRef[]` | Core | ✅ | — | 1-hop upstream; see Risk #5 re: lineage bounding decision |
| `depends_on[*].unique_id` | `string` | Core | ✅ | — | |
| `depends_on[*].edge_type` | `string` | Core | ✅ | — | |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | 1-hop downstream; see Risk #5 re: lineage bounding decision |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `execution_info` | `ExecutionInfo \| null` | Core-conditional | ✅ | `null` when run results absent | `null` when `dbt_rt.run_results` has no rows for this model |
| `execution_info.status` | `string \| null` | Core-conditional | ✅ | — | `"success"` · `"error"` · `"skipped"` |
| `execution_info.completed_at` | `string \| null` | Core-conditional | ✅ | — | Derived from `created_at`; space-separated local-timezone format (see Risk #1) |
| `execution_info.execution_time` | `number \| null` | Core-conditional | ✅ | — | Seconds (float) |
| `catalog` | `CatalogInfo \| null` | Core-conditional | ✅ | `null` when catalog absent | `null` when `dbt.catalog_tables` has no rows for this model |
| `catalog.type` | `string \| null` | Core-conditional | ✅ | — | `"table"` · `"view"` · `"materialized view"`; maps from `table_type` column |
| `catalog.owner` | `string \| null` | Core-conditional | ✅ | — | Warehouse role; maps from `table_owner` column |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔧 | — | Always `null`; lives in `dbt.catalog_stats` with adapter-specific `stat_id` (see Risk #4) |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔧 | — | Always `null`; same as above (see Risk #4) |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; Discovery-API-internal — see Risk #7 |
| `usage_query_count` | *(absent)* | — | ❌ | — | Class B: no parquet path; Discovery-API-internal |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface ModelDetail {
  unique_id: string;
  name: string;
  resource_type: "model";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  tags: string[];
  fqn: string[];
  materialized: string | null;
  database_name: string | null;
  schema_name: string | null;
  relation_name: string | null;
  identifier: string | null;
  access_level: string | null;
  group_name: string | null;
  contract_enforced: boolean | null;
  raw_code: string | null;
  compiled_code: string | null;
  columns: ModelColumn[];
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
  execution_info: ExecutionInfo | null;
  catalog: CatalogInfo | null;
}

interface ModelColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

interface ExecutionInfo {
  status: string | null;
  completed_at: string | null;
  execution_time: number | null;
}

interface CatalogInfo {
  type: string | null;
  owner: string | null;
  bytes_stat: number | null;
  row_count_stat: number | null;
}

interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`execution_info` implemented; `completed_at` format is approximate.** *(2026-05-18)*
   Implemented as a query against `dbt_rt.run_results` (not `run_results_latest` — no
   such pre-aggregated view found). Verified column names: `status` (Utf8), `execution_time`
   (Float64), `created_at` (timestamptz). `completed_at` is derived from `created_at` via
   `CAST(... AS VARCHAR)`, producing e.g. `"2026-05-14 17:41:56.652026-07"` (space separator,
   local timezone). The `timing` column holds a JSON array with per-phase UTC timestamps;
   extracting the execute-phase `completed_at` would give a cleaner ISO 8601 UTC value but
   adds DuckDB JSON path complexity. Deferred; acceptable for v0.

2. **`tags`, `fqn`, `contract_enforced` verified and implemented.** *(2026-05-18)*
   Confirmed against a real index: `tags` and `fqn` are native `List(Utf8)` columns —
   arrow_json serializes them as JSON arrays correctly. `contract_enforced` is a Boolean
   column. All three added to the handler SELECT.

3. **`compiled_code` confirmed present and implemented.** *(2026-05-18)*
   `compiled_code` is a `VARCHAR` column in `dbt.nodes` parquet. Enabled in the handler
   SELECT as `n.compiled_code`. For SQL models without macros, `compiled_code` equals
   `raw_code` (no template expansion needed). For models with `{{ ref(...) }}` calls,
   `compiled_code` contains the fully qualified SQL.

4. **Catalog column names corrected; `bytes_stat`/`row_count_stat` still open.** *(2026-05-18)*
   Verified `dbt.catalog_tables` schema: actual column names are `table_type` and
   `table_owner` (not `type`/`owner` as initially assumed — those would have caused 500s
   with real catalog data). Corrected in the handler. `bytes_stat` and `row_count_stat`
   do not exist in `catalog_tables`; they live in `dbt.catalog_stats` keyed by adapter-
   specific `stat_id` strings (e.g., `"bytes"`, `"num_bytes"` vary by adapter). Both
   are stubbed as `NULL::BIGINT` until a populated catalog index is available to confirm
   the stat IDs. The `catalog` object in the response will always have `null` for these
   two fields until that work is done.

5. **`depends_on`/`referenced_by` are intentionally unbounded.** *(Decision: 2026-05-18)*
   A `?first=` cap on a nested field is an API smell: every paginated request re-fetches
   all base fields as fixed overhead, and cursor state doesn't compose cleanly with a
   single-resource endpoint. The correct bounded path, if fan-out at scale requires it, is
   a dedicated lineage sub-resource (additive, backwards compatible). One caveat: silently
   **truncating** the inline arrays would be backwards incompatible by output even if the
   field name is preserved — clients that iterate `depends_on`/`referenced_by` would
   render incomplete graphs with no schema error to surface the problem. Therefore: keep
   unbounded until a sub-resource exists; never truncate the inline arrays without
   deprecating them first.

6. **`access_level` enum values need verification.** dbt-ui uses `AccessLevel`
   (`public | protected | private`). The current field is `string | null`. Confirm the
   string values match before the FE renders access badges to avoid silent mismatches.

7. **`health_issues` is Class B — no parquet path.** It is `subGraphs: ['internal']` in
   codex-api AND absent from all 34 parquet tables. The FE must render a graceful null
   state; do not add the field. Document explicitly so FE engineers don't chase it.

8. **Per-node test list has no handler coverage.** Requires a join across
   `dbt.test_metadata` and `dbt_rt.run_results`. **Open question:** inline a test summary
   array in `ModelDetail`, or introduce `GET /api/v1/models/:id/tests`? Resolve before
   implementing.

---

## ADR-3: `GET /api/v1/tests/:id` response shape for `test` vs `unit_test`

**Status:** Decided — single endpoint, discriminated union on `resource_type`.
**Trigger to revisit:** Unit tests and generic tests diverge enough to require a
dedicated UI page (currently both render via `TestView.tsx`).

### Context

`GET /api/v1/tests/:id` must serve two structurally distinct resource types:

| `resource_type` | `unique_id` prefix | Distinctive fields |
|---|---|---|
| `test` | `test.` | `column_name`, `test_metadata.kwargs`, `status`, `error` |
| `unit_test` | `unit_test.` | `given` rows, `expect` rows |

Both fold into the same "Tests" tab in dbt-ui (`TestView.tsx` handles both).
ADR-1 chose type-specific endpoints over a generic discriminated union across all
resource types — but `GET /api/v1/tests/:id` is itself a union of two sub-types.

### Options considered

**Single endpoint, discriminated union on `resource_type`**

`TestDetail` is `GenericTestDetail | UnitTestDetail`, narrowed by `resource_type`.
The two types share a common base and extend it with type-specific fields.

- Pro: one endpoint, one fetch per test detail page.
- Pro: both types are conceptually "test results" rendered on the same page — the
  distinction is an implementation detail, not a product-level one.
- Pro: consistent with how `GET /api/v1/nodes/:id` (deferred) would dispatch.
- Con: the response type is a union; FE must narrow on `resource_type`.

**Two endpoints: `/tests/:id` and `/unit_tests/:id`**

- Pro: fully separate types, no narrowing needed.
- Pro: strictest alignment with ADR-1's "one type per endpoint" principle.
- Con: FE must inspect `resource_type` (or parse the `unique_id` prefix) before
  routing to the right endpoint — the branching just moves to the call site.
- Con: tests and unit tests share a page and a concept; splitting endpoints
  diverges from the user's mental model.

→ **Single endpoint chosen.** There is no world where tests and unit tests belong
on separate detail pages. The union is an exception to ADR-1's general principle,
justified by the fact that `test` and `unit_test` are the same concept (test
coverage of a model) rendered identically in the UI.

---

## `GET /api/v1/nodes/:id` (deferred)

**Status:** Deferred — no v0 UI use case identified.
**Trigger to add:** MCP is added to dbt-docs-server.

When added: parse the `unique_id` prefix (`model.` → models handler, `source.` →
sources handler, etc.) and delegate to the appropriate typed handler. No logic
duplication. OpenAPI response type: `oneOf [ModelDetail, SourceDetail, ...]` with
`resource_type` as discriminator.

**Precondition:** `NodeBase` struct must already exist (ADR-1 backend prerequisite).

---

## ADR-4: `execution_info` field naming — single-run semantics

**Status:** Decided — bare field names without `last_run_*` or phase-scoped prefixes.
**Trigger to revisit:** Multi-run history (last N runs) becomes a product requirement.

dbt-docs-server is a **snapshot server**, not a history server. Every query reflects a
single indexed state: the output of one `dbt build` / `dbt seed` / `dbt snapshot` run
captured in parquet files at `<target>/index/`. There is no run timeline, no "previous
run" to contrast with a "last run." The word "last" implies a sequence; dbt-docs-server
exposes only the current snapshot.

Discovery API fields like `lastRunStatus`, `executeCompletedAt`, and `lastRunError` carry
prefixes because CodexDB has access to full run history. Importing those prefixes into
dbt-docs-server would be semantically misleading — `last_run_status` implies there could
be a `second_to_last_run_status`. The `execute_` prefix on `executeCompletedAt` is an
internal timing-phase name (compile vs. execute) that has no relevance to API consumers.

`lastKnownResult` (Discovery API for tests) tracks whether a test passed *before* a schema
change invalidated it — a concept that requires run history to be meaningful. In
dbt-docs-server the index is always one coherent snapshot: either the test ran and `status`
reflects the result, or the test hasn't run and `execution_info` is `null`. The "known vs.
actual" distinction does not exist and the field is **dropped**.

### Decision

`execution_info` fields use bare names:

| Discovery API field | dbt-docs-server field | Reason |
|---|---|---|
| `lastRunStatus` | `status` | No "last" — only one indexed run |
| `executeCompletedAt` | `completed_at` | `execute_` is an internal timing phase name |
| `lastRunError` | `error` | Same prefix problem |
| `lastKnownResult` | *(dropped)* | Requires run history; meaningless in snapshot world |

### If multi-run history is ever required

The path is a **new `runs[]` sub-resource**, not retrofitting `last_*` prefixes onto
existing fields. For example: `GET /api/v1/models/:id/runs` returns `Run[]` where each
`Run` has `status`, `completed_at`, `error`, `execution_time`. The inline `execution_info`
on the detail response becomes a convenience shortcut for the most-recent entry. This is
an additive change with no breaking impact on existing contracts.

---

## ADR-5: `execution_info` omission for definition-only and Semantic Layer resources

**Status:** Decided — `execution_info` is **omitted entirely** (not null-gated) on detail responses for resource types that never produce a `dbt_rt.run_results` row.
**Trigger to revisit:** dbt-mantle's Semantic Layer service exposes per-resource SL query history that can be backfilled into `dbt_rt.run_results` for `metric.*` / `saved_query.*` / `semantic_model.*` unique_ids.

### Why a new ADR

ADR-2 settled the *placement* of `execution_info` (inline, null-gated by `has_run_results`). ADR-2 implicitly assumed every resource type runs — true for models, sources (as test parents), seeds, snapshots, tests. False for the six resource types contracted in this PR.

A strict reading of ADR-2 would force `execution_info: null` onto every exposure / group / macro / metric / saved_query / semantic_model response in perpetuity, with no project state that would ever flip it to non-null. That's a "this field is dead by construction" surface — the FE has to render the null branch, the TypeScript type has to carry the optional, and reviewers have to wonder if `has_run_results: true` is supposed to populate it.

The honest contract is to omit the field entirely from the type and the wire response, the way `SourceDetail` omits `depends_on` (sources have no upstream) and `SeedDetail` omits `materialized` (seeds aren't materialized in the dbt sense). This ADR codifies that.

### Decision

The following resource types **do not carry `execution_info` on their detail response**, do not participate in the `has_run_results` capability gate, and do not have a `Core-conditional` row for run state in their field reference:

| Resource type | Why no `execution_info` |
|---|---|
| `exposure` | Not executed by dbt; declarative YAML pointing at downstream consumers (BI tools, ML jobs). No `dbt_rt.run_results` row. |
| `group` | Definition-only metadata. `dbt build` emits run results for member models, not for the group itself. |
| `macro` | Template; never materialized. The macro's *invocations* run; the macro doesn't. |
| `metric` | Semantic Layer definition. Executed at SL-query time against `dbt-mantle`, not by `dbt build`. |
| `saved_query` | Semantic Layer definition. Same execution model as `metric`. |
| `semantic_model` | Spec-only. Declares entities/dimensions/measures on top of an existing model; not itself executed. |

The "last updated" header timestamp these resources would otherwise want from `execution_info.completed_at` is sourced from `created_at` (epoch seconds, from each table's `dbt.<table>.created_at` column). Groups fall back to `ingested_at` (no `created_at` column on `dbt.groups`).

### Backend prerequisite — `NodeBase` split

ADR-1 required that all typed detail handlers compose a shared `NodeBase` struct. Now that ADR-5 carves out a class of resources that don't carry `execution_info` (and groups don't even carry `fqn`), `NodeBase` is split into three: a shared `NodeBase` for fields every resource type has, a `RunnableNodeBase` that adds `tags`, `fqn`, and `execution_info` for resource types that `dbt build` actually runs, and a `DefinitionNodeBase` that adds `tags`, `fqn`, and `created_at` for definition-only resources (`GroupDetail` composes `NodeBase` directly because `dbt.groups` has no `fqn`).

```rust
// Common to every resource type.
struct NodeBase {
    unique_id: String,
    name: String,
    resource_type: String,
    package_name: Option<String>,
    description: Option<String>,
    original_file_path: Option<String>,
}

// Composed by ModelDetail, SourceDetail, SeedDetail, SnapshotDetail, TestDetail.
// Carries the fields that only apply to resources dbt actually runs.
struct RunnableNodeBase {
    #[serde(flatten)] base: NodeBase,
    tags: Vec<String>,
    fqn: Vec<String>,
    execution_info: Option<ExecutionInfo>,   // null-gated by has_run_results
}

// Composed by ExposureDetail, MacroDetail, MetricDetail, SavedQueryDetail,
// SemanticModelDetail. GroupDetail composes NodeBase directly (no fqn either).
struct DefinitionNodeBase {
    #[serde(flatten)] base: NodeBase,
    tags: Vec<String>,           // sourced from `config` JSON for some resources
    fqn: Vec<String>,            // empty for groups
    created_at: Option<f64>,     // epoch seconds; groups fall back to ingested_at
}
```

### What this changes in the per-endpoint contracts

Each ADR-5–scoped endpoint's field reference table:
- **may** include an `❌ absent` row for `execution_info` documenting *why* it's not in the response shape (preserves the audit trail; the field is still absent from `DefinitionNodeBase`, the example response, and the TypeScript type). Existing contracts use this pattern — keep it.
- **must** include a `created_at: number | null` row (Core 🔧) sourced from the resource's parquet `created_at` column. **Exception:** `dbt.groups` has no `created_at` column — the groups contract surfaces `ingested_at` instead, with a row note documenting the fallback.

The per-endpoint Design notes for each of the six resource types that flag "no execution_info because definition-only" are now redundant with this ADR; they remain in place as supporting context but reviewers should treat ADR-5 as the source of truth.

---

## `GET /api/v1/sources/:id`

Powers: `SourceView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SourceView.tsx`
GraphQL hooks: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useSource.ts` → `src/hooks/discovery/source.ts` (`GetSourceByUniqueId`)

**No new ADR needed.** This endpoint follows ADR-1 (type-specific) and ADR-2 (conditional
data inlined, null-gated by capability) without exception. `freshness` replaces
`execution_info` as the Core-conditional surface for sources.

### Example response

`freshness` is `null` when `has_source_freshness` is false (i.e., `dbt source freshness` has not run).
`catalog` is `null` when `has_catalog_stats` is false (i.e., `dbt docs generate` has not run).
Fields marked `// 🔧` are not yet returned — they require a backend change.

```json
{
  "unique_id": "source.jaffle_shop.raw_jaffle.orders",
  "name": "orders",
  "resource_type": "source",
  "package_name": "jaffle_shop",
  "description": "Raw orders table from the production Postgres database.",
  "original_file_path": "models/staging/sources.yml",
  "file_path": "models/staging/sources.yml",
  "tags": ["raw", "jaffle"],
  "fqn": ["jaffle_shop", "raw_jaffle", "orders"],
  "database_name": "raw",
  "schema_name": "jaffle_shop",
  "identifier": "orders",
  "source_name": "raw_jaffle",
  "source_description": "Raw tables synced from the Jaffle Shop production database.",
  "loader": "fivetran",
  "meta": { "owner": "data-eng" },
  "referenced_by": [
    { "unique_id": "model.jaffle_shop.stg_orders", "edge_type": "model" }
  ],
  "columns": [
    {
      "name": "id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique order identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "freshness": {
    "status": "pass",
    "snapshotted_at": "2026-05-15T10:00:00Z",
    "max_loaded_at": "2026-05-15T09:45:00Z",
    "max_loaded_at_time_ago": 900.0,
    "criteria": {
      "error_after": { "count": 24, "period": "hour" },
      "warn_after": { "count": 12, "period": "hour" }
    }
  },
  "catalog": {
    "type": "table",
    "owner": "fivetran",
    "comment": "Raw orders synced from production PostgreSQL.",
    "primary_key": ["id"],
    "row_count_stat": 50000,
    "bytes_stat": 2097152,
    "stats": [
      {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": "true",
        "description": "Indicates whether there are statistics for this table",
        "include": false
      }
    ]
  }
}
```

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"source.pkg.source_name.table_name"` — 4-part unique_id |
| `name` | `string` | Core | ✅ | — | Table name within the source block |
| `resource_type` | `"source"` | Core | ✅ | — | Always `"source"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | Per-table description from YAML |
| `original_file_path` | `string \| null` | Core | ✅ | — | Path to the `.yml` file containing the source definition |
| `file_path` | `string \| null` | Core | 🔧 | — | From `dbt.nodes.file_path`; for sources equals `original_file_path` (the same `.yml`, since sources are YAML-only) |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; 3-part for sources: `[pkg, source_name, table]` |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `identifier` | `string \| null` | Core | ✅ | — | Overrides table name if set; falls back to `name` |
| `source_name` | `string \| null` | Core | 🔧 | — | dbt source block name (e.g., `"raw_jaffle"`) — in `dbt.nodes` parquet |
| `source_description` | `string \| null` | Core | 🔧 | — | Block-level description from YAML — in `dbt.nodes` parquet |
| `loader` | `string \| null` | Core | 🔧 | — | e.g., `"fivetran"`, `"airbyte"` — in `dbt.nodes` parquet |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob — confirm `dbt.nodes` parquet includes a `meta` column |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | Downstream models; sources have **no** `depends_on` |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `columns` | `SourceColumn[]` | Core | ✅ | — | Identical shape to `ModelColumn[]` |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `has_catalog_stats` | Warehouse-verified type |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | |
| `freshness` | `FreshnessInfo \| null` | Core-conditional | 🔧 | `has_source_freshness` | `null` if `dbt source freshness` hasn't run — see Risk #2 |
| `freshness.status` | `string` | Core-conditional | 🔧 | `has_source_freshness` | `"pass"` · `"warn"` · `"error"` · `"runtime error"` |
| `freshness.snapshotted_at` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | ISO 8601; when freshness was last checked |
| `freshness.max_loaded_at` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | ISO 8601; most recent row timestamp from the source table |
| `freshness.max_loaded_at_time_ago` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | Seconds elapsed since `max_loaded_at` |
| `freshness.criteria.error_after.count` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | |
| `freshness.criteria.error_after.period` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | `"minute"` · `"hour"` · `"day"` |
| `freshness.criteria.warn_after.count` | `number \| null` | Core-conditional | 🔧 | `has_source_freshness` | |
| `freshness.criteria.warn_after.period` | `string \| null` | Core-conditional | 🔧 | `has_source_freshness` | `"minute"` · `"hour"` · `"day"` |
| `catalog` | `SourceCatalogInfo \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Superset of model `CatalogInfo` — adds `comment`, `primary_key`, `stats[]` |
| `catalog.type` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.owner` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.comment` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse table comment — source-only field |
| `catalog.primary_key` | `string[]` | Core-conditional | 🔧 | `has_catalog_stats` | Column names constituting the PK; empty array if none. Sourced from `dbt.nodes.primary_key` (a `List<String>` column) — not from `dbt.catalog_tables`, which has no `primary_key` column — source-only field |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats` | `CatalogStat[]` | Core-conditional | 🔧 | `has_catalog_stats` | Arbitrary warehouse statistics — source-only field |
| `catalog.stats[*].id` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Stat identifier |
| `catalog.stats[*].label` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Human-readable label |
| `catalog.stats[*].value` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Always a string; parse as number if needed |
| `catalog.stats[*].description` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].include` | `boolean` | Core-conditional | 🔧 | `has_catalog_stats` | Whether the stat should be displayed in the UI |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |
| `patch_path` | *(absent)* | — | ❌ | — | Class B: YAML-only resource — `original_file_path` IS the `.yml` file containing the source definition; the patch concept does not apply (a "patch" is a separate YAML that augments a non-YAML primary definition, e.g. `.sql` + `schema.yml`). Discovery's `patchPath` would be null or duplicate `originalFilePath` for this resource. |

**Fields absent from `SourceDetail` that exist on `ModelDetail`:**
Sources have no SQL, no materialization strategy, no dbt-managed relation, and no run
execution. The following fields from `ModelDetail` are intentionally omitted:
`materialized`, `relation_name`, `access_level`, `group_name`, `contract_enforced`,
`raw_code`, `compiled_code`, `depends_on`, `execution_info`.

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SourceDetail {
  unique_id: string;
  name: string;
  resource_type: "source";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  tags: string[];
  fqn: string[];
  database_name: string | null;
  schema_name: string | null;
  identifier: string | null;
  source_name: string | null;
  source_description: string | null;
  loader: string | null;
  meta: Record<string, unknown> | null;
  referenced_by: EdgeRef[];
  columns: SourceColumn[];
  freshness: FreshnessInfo | null;
  catalog: SourceCatalogInfo | null;
}

// SourceColumn is identical in shape to ModelColumn
interface SourceColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

interface FreshnessInfo {
  status: string;
  snapshotted_at: string | null;
  max_loaded_at: string | null;
  max_loaded_at_time_ago: number | null;
  criteria: {
    error_after: { count: number | null; period: string | null } | null;
    warn_after: { count: number | null; period: string | null } | null;
  } | null;
}

// SourceCatalogInfo extends model CatalogInfo with source-specific fields
interface SourceCatalogInfo {
  type: string | null;
  owner: string | null;
  comment: string | null;
  primary_key: string[];
  row_count_stat: number | null;
  bytes_stat: number | null;
  stats: CatalogStat[];
}

interface CatalogStat {
  id: string;
  label: string;
  value: string;
  description: string;
  include: boolean;
}

// EdgeRef is shared with ModelDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`source_name`, `source_description`, `loader` not yet queried.** These are
   source-specific fields in `dbt.nodes` parquet that aren't in the current handler
   SELECT. Add them alongside `tags`, `fqn`, and `contract_enforced` in the same
   handler change.

2. **Freshness parquet coexistence is unverified.** `dbt.source_freshness.parquet`
   contains all the fields needed, but it's written by a separate command (`dbt source
   freshness`) from the one that writes `dbt.nodes.parquet` (`dbt build` / `dbt parse`).
   The question from FEATURE-TO-ENDPOINT-MAPPING.md Appendix B1: does `dbt --use-index
   source freshness` use MergePrune semantics that preserve `nodes.parquet`, or does it
   overwrite the index directory? If it overwrites: freshness is not available in
   stateless docs and must be treated as Platform-tier. **Verify with the dbt-index team
   before implementing `has_source_freshness`.**

3. **`meta` JSONB presence in parquet is unverified.** The `meta` field is a JSONB
   object in codex-api's Prisma schema. Confirm it's serialized into `dbt.nodes.parquet`
   as a JSON string column before adding it to the SELECT.

4. **`SourceCatalogInfo` is a superset of `CatalogInfo`.** The model catalog type
   (`CatalogInfo`) does not include `comment`, `primary_key`, or `stats[]`. The Rust
   handler will need a separate response struct for source catalog data, or `CatalogInfo`
   must be extended. Decide before implementing to avoid a breaking change to `ModelDetail`.

5. **`catalog.stats[]` schema is warehouse-dependent.** The stat entries (e.g.,
   `has_stats`, `row_count`, `bytes`) vary by adapter. The `value` field is always a
   string regardless of the underlying type — document this explicitly so FE engineers
   don't attempt numeric parsing without a string-to-number conversion.

6. **Sources have no `depends_on`.** The current `NodeDetail` handler always returns
   both `depends_on` and `referenced_by`. The `SourceDetail` handler must omit
   `depends_on` entirely (not return an empty array) to avoid FE engineers
   misinterpreting an empty array as "no upstream sources found."

## `GET /api/v1/seeds/:id`

Powers: `SeedView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SeedView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/seed.ts` (`GetSeedByUniqueId`)

Seeds are CSV files loaded into the warehouse by `dbt seed` / `dbt build`. They share
the `dbt.nodes` parquet row structure with models and snapshots, but have no SQL body
(`raw_code` and `compiled_code` absent), no materialization strategy (`materialized`
absent), and no upstream dependencies (`depends_on` omitted — not an empty array).
Seeds DO have `execution_info`, `columns`, and `catalog`. `identifier` maps to
`dbt.nodes.alias` for seeds (the field that overrides the CSV filename to set the
warehouse table name). The per-seed `tests[]` inline array is deferred to a future
`GET /api/v1/seeds/:id/tests` sub-resource — mirrors the open question from
`ModelDetail` Risk #8.

### Example response

Fields marked `// conditional` are `null` when their capability gate is absent.
Fields marked `// 🔧` are not yet returned — they require a backend change.
Fields marked `// 🔍` are parquet-unverified — confirm schema before implementing.

```json
{
  "unique_id": "seed.jaffle_shop.raw_customers",
  "name": "raw_customers",
  "resource_type": "seed",
  "package_name": "jaffle_shop",
  "description": "Raw customer seed file loaded from CSV.",
  "original_file_path": "seeds/raw_customers.csv",
  "file_path": "raw_customers.csv",
  "patch_path": "seeds/_schema.yml",
  "tags": ["raw", "seed"],
  "fqn": ["jaffle_shop", "raw_customers"],
  "database_name": "prod",
  "schema_name": "dbt_prod",
  "identifier": "raw_customers",
  "meta": { "owner": "data-eng" },
  "columns": [
    {
      "name": "id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique customer identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "referenced_by": [
    { "unique_id": "model.jaffle_shop.stg_customers", "edge_type": "ref" }
  ],
  "execution_info": {
    "status": "success",
    "completed_at": "2026-05-15T10:28:03Z",
    "execution_time": 1.8
  },
  "catalog": {
    "type": "table",
    "owner": "dbt_runner",
    "row_count_stat": 935,
    "bytes_stat": 49152,
    "stats": [
      {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": "true",
        "description": "Indicates whether there are statistics for this table",
        "include": false
      }
    ]
  }
}
```

`execution_info` is `null` when `has_run_results` is false (i.e., `dbt seed` / `dbt build` has not run).
`catalog` is `null` when `has_catalog_stats` is false (i.e., `dbt docs generate` has not run).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"seed.pkg.name"` |
| `name` | `string` | Core | ✅ | — | |
| `resource_type` | `"seed"` | Core | ✅ | — | Always `"seed"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | |
| `original_file_path` | `string \| null` | Core | ✅ | — | Path to the CSV file relative to project root |
| `file_path` | `string \| null` | Core | 🔧 | — | Relative path from `dbt.nodes.file_path`; used by UI "Files" list |
| `patch_path` | `string \| null` | Core | 🔧 | — | Path to the YAML schema patch file, project-relative (no `<package>://` prefix — `dbt.nodes.patch_path` stores the bare path) — in `dbt.nodes` parquet |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `identifier` | `string \| null` | Core | ✅ | — | Maps to `dbt.nodes.alias` for seeds; warehouse table name |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob — confirm `dbt.nodes` parquet includes a `meta` column |
| `columns` | `SeedColumn[]` | Core | ✅ | — | Empty array if `dbt docs generate` has not run |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | Column order |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML patch |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `has_catalog_stats` | Warehouse-verified type; `null` unless `dbt docs generate` ran |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | Downstream models; seeds have **no** `depends_on` |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `execution_info` | `ExecutionInfo \| null` | Core-conditional | 🔧 | `has_run_results` | `null` when `dbt seed` / `dbt build` hasn't run |
| `execution_info.status` | `string` | Core-conditional | 🔧 | `has_run_results` | `"success"` · `"error"` · `"skipped"` |
| `execution_info.completed_at` | `string \| null` | Core-conditional | 🔍 | `has_run_results` | ISO 8601; extracted from `timing` JSON column — requires `json_extract_string` over the `timing` array |
| `execution_info.execution_time` | `number \| null` | Core-conditional | 🔧 | `has_run_results` | Seconds (float); from `dbt_rt.run_results.execution_time` |
| `catalog` | `SeedCatalogInfo \| null` | Core-conditional | 🔧 | `has_catalog_stats` | `null` when `dbt docs generate` hasn't run |
| `catalog.type` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse object type; seeds are always `"table"` |
| `catalog.owner` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse role that owns the relation |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔍 | `has_catalog_stats` | Approximate row count; from `dbt.catalog_stats` — confirm stat key |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔍 | `has_catalog_stats` | Bytes; from `dbt.catalog_stats` — confirm stat key |
| `catalog.stats` | `CatalogStat[]` | Core-conditional | 🔧 | `has_catalog_stats` | Arbitrary warehouse statistics |
| `catalog.stats[*].id` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Stat identifier |
| `catalog.stats[*].label` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Human-readable label |
| `catalog.stats[*].value` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Always a string; parse as number if needed |
| `catalog.stats[*].description` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].include` | `boolean` | Core-conditional | 🔧 | `has_catalog_stats` | Whether the stat should be displayed in the UI |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud concept; not in parquet |
| `last_run_id` | *(absent)* | — | ❌ | — | Class B: Cloud run ID; not in local parquet |
| `last_job_definition_id` | *(absent)* | — | ❌ | — | Class B: Cloud scheduler concept; not in parquet |
| `raw_code` | *(absent)* | — | ❌ | — | Seeds have no SQL body |
| `compiled_code` | *(absent)* | — | ❌ | — | Seeds have no SQL body |
| `materialized` | *(absent)* | — | ❌ | — | Seeds are always a table; no strategy field |
| `access_level` | *(absent)* | — | ❌ | — | Model-access feature; not applicable to seeds |
| `group_name` | *(absent)* | — | ❌ | — | Not applicable to seeds |
| `contract_enforced` | *(absent)* | — | ❌ | — | Not applicable to seeds |
| `relation_name` | *(absent)* | — | ❌ | — | Not emitted by dbt for seeds; `identifier` covers the use case |
| `depends_on` | *(absent)* | — | ❌ | — | Seeds have no upstream dependencies; omit entirely (not empty array) |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |

`SeedCatalogInfo` omits `comment` and `primary_key` (source-only fields) and is
structurally identical to the base `CatalogInfo` from `ModelDetail`, extended with `stats[]`.

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SeedDetail {
  unique_id: string;
  name: string;
  resource_type: "seed";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  patch_path: string | null;
  tags: string[];
  fqn: string[];
  database_name: string | null;
  schema_name: string | null;
  identifier: string | null;
  meta: Record<string, unknown> | null;
  columns: SeedColumn[];
  referenced_by: EdgeRef[];
  execution_info: ExecutionInfo | null;
  catalog: SeedCatalogInfo | null;
}

// SeedColumn is identical in shape to ModelColumn and SourceColumn
interface SeedColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

// SeedCatalogInfo extends base CatalogInfo with stats[]
// Does NOT include comment or primary_key (those are SourceCatalogInfo-only)
interface SeedCatalogInfo {
  type: string | null;
  owner: string | null;
  row_count_stat: number | null;
  bytes_stat: number | null;
  stats: CatalogStat[];
}

// ExecutionInfo, CatalogStat, EdgeRef are shared with ModelDetail
```

### Risk register

1. **`file_path` and `patch_path` are not queried by the existing handler.** Both columns
   exist in `dbt.nodes` parquet (confirmed in `upsert_node`). Add them to the seed-specific
   handler SELECT alongside `tags`, `fqn`, and `alias`.

2. **`completed_at` requires JSON extraction from `timing`.** The `dbt_rt.run_results`
   table stores timing data as a JSON array in the `timing` column. Extraction requires a
   DuckDB JSON path expression, not a simple column alias. Confirm the exact syntax against a
   real index before implementing. If the execute phase is missing, return `null`.

3. **`meta` JSONB presence in parquet is unverified.** Same risk as documented in the source
   contract (Risk #3). Confirm the `meta` column is queryable in `dbt.nodes.parquet` before
   adding it to the SELECT.

4. **Per-seed test list is deferred.** The GraphQL query fetches `tests[]` inline, powering
   the `useSetMissingTests` warning banner in `SeedView`. Defer to a future
   `GET /api/v1/seeds/:id/tests` sub-resource. FE must render a graceful null state until
   that endpoint exists.

5. **`catalog.row_count_stat` and `catalog.bytes_stat` stat key names are unverified.**
   These values live in `dbt.catalog_stats` keyed by `stat_id`. Canonical stat IDs vary by
   adapter. Confirm the exact keys used by dbt-index catalog ingestion before mapping to
   top-level response fields. The raw `stats[]` array is the safe fallback.

6. **`depends_on` must be omitted, not empty.** Seeds have no upstream SQL dependencies.
   The handler must NOT return `depends_on: []` — omit the field entirely. Consistent with
   `SourceDetail` precedent.

7. **`SeedCatalogInfo` catalog struct alignment.** Three distinct catalog shapes now exist:
   `CatalogInfo` (models), `SourceCatalogInfo` (adds `comment`, `primary_key`, `stats[]`),
   `SeedCatalogInfo` (adds `stats[]` only). Decide before implementation whether to define
   separate Rust structs or unify into a single struct with nullable extension fields.

---

## `GET /api/v1/snapshots/:id`

Powers: `SnapshotView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SnapshotView.tsx`
GraphQL hooks: `packages/metadata/dbt-explorer/src/hooks/discovery/snapshot.ts` (`GetSnapshotByUniqueId`) and `src/hooks/dbtStrategy/useSnapshot.ts`

**No new ADR needed.** This endpoint follows ADR-1 (type-specific) and ADR-2 (conditional
data inlined, null-gated by capability) without exception. `execution_info` applies to
snapshots exactly as it does to models (`dbt build` and `dbt snapshot` both produce run
results). Snapshots share the model execution surface (`execution_info`, `catalog`,
`columns`, `depends_on`, `referenced_by`, `raw_code`, `compiled_code`) but add
`patch_path` (the `.yml` patch file, separate from `original_file_path`) and omit
model-only governance fields (`access_level`, `group_name`, `contract_enforced`). The
per-snapshot `tests[]` inline array is deferred — same open question as `ModelDetail`
Risk #8.

### Example response

Fields marked `// conditional` are `null` when their capability gate is absent.
Fields marked `// 🔧` are not yet returned — they require a backend change.
Fields marked `// 🔍` are parquet presence unverified.

```json
{
  "unique_id": "snapshot.jaffle_shop.orders_snapshot",
  "name": "orders_snapshot",
  "resource_type": "snapshot",
  "package_name": "jaffle_shop",
  "description": "Snapshot of the orders table tracking row-level changes over time.",
  "original_file_path": "snapshots/orders_snapshot.sql",
  "patch_path": "snapshots/schema.yml",
  "tags": ["finance", "snapshot"],
  "fqn": ["jaffle_shop", "orders_snapshot"],
  "database_name": "prod",
  "schema_name": "dbt_prod",
  "identifier": "orders_snapshot",
  "relation_name": "prod.dbt_prod.orders_snapshot",
  "materialized": "snapshot",
  "raw_code": "{%- snapshot orders_snapshot -%}\n  ...\n{%- endsnapshot -%}",
  "compiled_code": null,
  "meta": { "owner": "data-eng" },
  "depends_on": [
    { "unique_id": "model.jaffle_shop.orders", "edge_type": "model" }
  ],
  "referenced_by": [],
  "columns": [
    {
      "name": "order_id",
      "index": 0,
      "data_type": "integer",
      "declared_type": "int",
      "inferred_type": null,
      "catalog_type": "INT64",
      "description": "Unique order identifier.",
      "label": null,
      "granularity": null
    }
  ],
  "execution_info": {
    "status": "success",
    "completed_at": "2026-05-15T10:32:11Z",
    "execution_time": 12.7
  },
  "catalog": {
    "type": "table",
    "owner": "dbt_runner",
    "primary_key": ["order_id"],
    "row_count_stat": 42000,
    "bytes_stat": 3145728,
    "stats": [
      {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": "true",
        "description": "Indicates whether there are statistics for this table",
        "include": false
      }
    ]
  }
}
```

`execution_info` is `null` when `has_run_results` is false (i.e., `dbt build` has not run).
`catalog` is `null` when `has_catalog_stats` is false (i.e., `dbt docs generate` has not run).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | ✅ | — | e.g., `"snapshot.pkg.name"` |
| `name` | `string` | Core | ✅ | — | |
| `resource_type` | `"snapshot"` | Core | ✅ | — | Always `"snapshot"` for this endpoint |
| `package_name` | `string \| null` | Core | ✅ | — | |
| `description` | `string \| null` | Core | ✅ | — | |
| `original_file_path` | `string \| null` | Core | ✅ | — | Path to the `.sql` file; maps from `filePath` in GraphQL |
| `patch_path` | `string \| null` | Core | 🔍 | — | Path to the `.yml` patch file; in manifest but unverified in `dbt.nodes` parquet — see Risk #1 |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet; add to handler SELECT |
| `database_name` | `string \| null` | Core | ✅ | — | |
| `schema_name` | `string \| null` | Core | ✅ | — | |
| `identifier` | `string \| null` | Core | ✅ | — | Maps from `alias` in GraphQL; overrides `name` if set |
| `relation_name` | `string \| null` | Core | ✅ | — | Fully qualified: `db.schema.name` |
| `materialized` | `"snapshot"` | Core | ✅ | — | Always `"snapshot"` for this resource type |
| `raw_code` | `string \| null` | Core | ✅ | — | The `{%- snapshot -%}` block source |
| `compiled_code` | `string \| null` | Core | 🔍 | — | Likely in `dbt.nodes` parquet — confirm schema before implementing; see Risk #2 |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob — confirm `dbt.nodes` parquet includes a `meta` column; see Risk #3 |
| `depends_on` | `EdgeRef[]` | Core | ✅ | — | 1-hop upstream; see Risk #4 re: pagination |
| `depends_on[*].unique_id` | `string` | Core | ✅ | — | |
| `depends_on[*].edge_type` | `string` | Core | ✅ | — | |
| `referenced_by` | `EdgeRef[]` | Core | ✅ | — | 1-hop downstream; see Risk #4 re: pagination |
| `referenced_by[*].unique_id` | `string` | Core | ✅ | — | |
| `referenced_by[*].edge_type` | `string` | Core | ✅ | — | |
| `columns` | `SnapshotColumn[]` | Core | ✅ | — | Identical shape to `ModelColumn[]`; empty array if none declared |
| `columns[*].name` | `string` | Core | ✅ | — | |
| `columns[*].index` | `number \| null` | Core | ✅ | — | Column order |
| `columns[*].data_type` | `string \| null` | Core | ✅ | — | Declared in YAML |
| `columns[*].declared_type` | `string \| null` | Core | ✅ | — | |
| `columns[*].inferred_type` | `string \| null` | Proprietary | ✅ | — | `null` in Core; populated by Fusion static analysis |
| `columns[*].catalog_type` | `string \| null` | Core-conditional | ✅ | `has_catalog_stats` | Warehouse-verified type; `null` unless `dbt docs generate` ran |
| `columns[*].description` | `string \| null` | Core | ✅ | — | |
| `columns[*].label` | `string \| null` | Core | ✅ | — | |
| `columns[*].granularity` | `string \| null` | Core | ✅ | — | |
| `execution_info` | `ExecutionInfo \| null` | Core-conditional | 🔧 | `has_run_results` | `null` when `dbt build` hasn't run |
| `execution_info.status` | `string` | Core-conditional | 🔧 | `has_run_results` | `"success"` · `"error"` · `"skipped"` |
| `execution_info.completed_at` | `string \| null` | Core-conditional | 🔧 | `has_run_results` | ISO 8601 timestamp |
| `execution_info.execution_time` | `number \| null` | Core-conditional | 🔧 | `has_run_results` | Seconds (float) |
| `catalog` | `SnapshotCatalogInfo \| null` | Core-conditional | 🔧 | `has_catalog_stats` | `null` when `dbt docs generate` hasn't run; adds `primary_key` and `stats[]` over base `CatalogInfo` |
| `catalog.type` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | `"table"` · `"view"` · `"materialized view"` |
| `catalog.owner` | `string \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Warehouse role that owns the relation |
| `catalog.primary_key` | `string[]` | Core-conditional | 🔧 | `has_catalog_stats` | Column names constituting the PK; empty array if none. Sourced from `dbt.nodes.primary_key` (a `List<String>` column, populated from the snapshot's `unique_key` config) — not from `dbt.catalog_tables`, which has no `primary_key` column |
| `catalog.row_count_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Approximate row count |
| `catalog.bytes_stat` | `number \| null` | Core-conditional | 🔧 | `has_catalog_stats` | Bytes; warehouse-specific |
| `catalog.stats` | `CatalogStat[]` | Core-conditional | 🔧 | `has_catalog_stats` | Arbitrary warehouse statistics; same shape as `SourceCatalogInfo.stats[]` |
| `catalog.stats[*].id` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].label` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].value` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | Always string; parse as number if needed |
| `catalog.stats[*].description` | `string` | Core-conditional | 🔧 | `has_catalog_stats` | |
| `catalog.stats[*].include` | `boolean` | Core-conditional | 🔧 | `has_catalog_stats` | Whether to display in UI |
| `tests` | *(absent)* | — | ❌ | — | Deferred for v0; same open question as `ModelDetail` Risk #8 — defer until model contract resolves |
| `access_level` | *(absent)* | — | ❌ | — | Model-only governance field; not applicable to snapshots |
| `group_name` | *(absent)* | — | ❌ | — | Model-only governance field; not applicable to snapshots |
| `contract_enforced` | *(absent)* | — | ❌ | — | Model-only governance field; not applicable to snapshots |
| `last_run_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific run ID; no parquet path |
| `last_job_definition_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific job ID; no parquet path |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific; no parquet path |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; Discovery-API-internal |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SnapshotDetail {
  unique_id: string;
  name: string;
  resource_type: "snapshot";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  patch_path: string | null;
  tags: string[];
  fqn: string[];
  database_name: string | null;
  schema_name: string | null;
  identifier: string | null;
  relation_name: string | null;
  materialized: "snapshot";
  raw_code: string | null;
  compiled_code: string | null;
  meta: Record<string, unknown> | null;
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
  columns: SnapshotColumn[];
  execution_info: ExecutionInfo | null;
  catalog: SnapshotCatalogInfo | null;
}

// SnapshotColumn is identical in shape to ModelColumn
interface SnapshotColumn {
  name: string;
  index: number | null;
  data_type: string | null;
  declared_type: string | null;
  inferred_type: string | null;
  catalog_type: string | null;
  description: string | null;
  label: string | null;
  granularity: string | null;
}

// SnapshotCatalogInfo adds primary_key and stats[] over model CatalogInfo
// (matches SourceCatalogInfo minus the comment field)
interface SnapshotCatalogInfo {
  type: string | null;
  owner: string | null;
  primary_key: string[];
  row_count_stat: number | null;
  bytes_stat: number | null;
  stats: CatalogStat[];
}

// ExecutionInfo, CatalogStat, EdgeRef are shared with ModelDetail
```

### Risk register

1. **`patch_path` presence in parquet is unverified.** `SnapshotView.tsx` reads
   `snapshot.applied.patchPath` to populate the file link in the resource header. The field
   exists in `manifest.json` and the GraphQL applied-state layer, but whether dbt-index
   writes it into `dbt.nodes.parquet` is unconfirmed. Verify before adding to the handler
   SELECT. If absent from parquet, the field must be omitted or gated on a new capability.

2. **`compiled_code` presence in parquet is unverified.** Snapshots use `{%- snapshot -%}`
   blocks and dbt does compile them; the compiled form is likely present but needs
   confirmation. Mark as TODO and omit if absent.

3. **`meta` JSONB presence in parquet is unverified.** Same risk as `SourceDetail` Risk #3.
   Confirm the column is serialized as a queryable JSON string in `dbt.nodes.parquet` before
   adding to the SELECT.

4. **`depends_on`/`referenced_by` have no pagination cap.** Identical risk to `ModelDetail`
   Risk #5. Add a `?first=` cap with `truncated: true` for v0.

5. **`SnapshotCatalogInfo` vs. `CatalogInfo` struct proliferation.** Snapshots and sources
   both extend base `CatalogInfo` with `primary_key` and `stats[]`; sources also add
   `comment`. Three distinct catalog structs now exist. Decide at implementation time whether
   to unify into a single struct with nullable extension fields, or keep separate Rust structs.
   The decision affects all three existing contracts.

6. **`tests[]` inline deferred — surface may never be added.** Block on the model-level
   resolution of `ModelDetail` Risk #8 to keep contracts consistent.

7. **`execution_info` absent from current handler.** The existing generic `get_node` handler
   does not query `dbt_rt.run_results_latest`. The snapshot-specific handler will need this
   query added behind the `has_run_results` capability flag.

---

## `GET /api/v1/tests/:id`

Powers: `TestView` / `ResourceDetailsPage` in dbt-ui — header card (type, last run status,
target column), Code tab (raw + compiled SQL for data tests; given/expect YAML for unit
tests), General tab (description, metadata, dependencies).

dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/TestView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useTest.ts` → `src/hooks/discovery/test.ts` (`GetTestByUniqueId`)

This endpoint covers **both** `test.*` and `unit_test.*` unique_ids. The response shape is
a discriminated union on `resource_type` as decided in ADR-3.

### Example response (data test)

`execution_info` is `null` when `has_run_results` is false (i.e., `dbt build` has not run).
Fields marked `// 🔧` are not yet returned — they require a backend change.
Fields marked `// 🔍` are in parquet but the exact column name is unconfirmed.

```json
{
  "unique_id": "test.jaffle_shop.not_null_orders_order_id",
  "name": "not_null_orders_order_id",
  "resource_type": "test",
  "package_name": "jaffle_shop",
  "description": "Asserts that order_id is never null.",
  "original_file_path": "models/schema.yml",
  "tags": ["data-quality"],
  "fqn": ["jaffle_shop", "not_null_orders_order_id"],
  "column_name": "order_id",
  "test_type": "generic",
  "severity": "ERROR",
  "test_metadata": {
    "name": "not_null",
    "kwargs": { "column_name": "order_id", "model": "ref('orders')" }
  },
  "raw_code": "select order_id from {{ model }} where order_id is null",
  "compiled_code": "select order_id from prod.dbt_prod.orders where order_id is null",
  "file_path": "models/schema.yml",
  "patch_path": null,
  "meta": {},
  "depends_on": [
    { "unique_id": "model.jaffle_shop.orders", "edge_type": "model" }
  ],
  "execution_info": {
    "status": "pass",
    "error": null,
    "completed_at": "2026-05-15T10:32:11Z",
    "execution_time": 1.4
  }
}
```

### Example response (unit test)

`execution_info` is `null` when `has_run_results` is false.

```json
{
  "unique_id": "unit_test.jaffle_shop.test_orders_completed_status",
  "name": "test_orders_completed_status",
  "resource_type": "unit_test",
  "package_name": "jaffle_shop",
  "description": "Checks that completed orders always have a non-null amount.",
  "original_file_path": "models/schema.yml",
  "tags": [],
  "fqn": ["jaffle_shop", "test_orders_completed_status"],
  "model": "ref('orders')",
  "given": [
    {
      "input": "ref('stg_orders')",
      "rows": [
        { "order_id": 1, "status": "completed" },
        { "order_id": 2, "status": "pending" }
      ]
    }
  ],
  "expect": {
    "rows": [
      { "order_id": 1, "amount": 25.00 }
    ]
  },
  "num_given": 1,
  "num_given_rows": 2,
  "num_expect_rows": 1,
  "file_path": "models/schema.yml",
  "patch_path": null,
  "meta": {},
  "depends_on": [
    { "unique_id": "model.jaffle_shop.orders", "edge_type": "model" },
    { "unique_id": "model.jaffle_shop.stg_orders", "edge_type": "model" }
  ],
  "execution_info": {
    "status": "pass",
    "error": null,
    "completed_at": "2026-05-15T10:32:15Z",
    "execution_time": 0.8
  }
}
```

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

Fields that appear in only one variant are noted in the Notes column.

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"test.pkg.name"` or `"unit_test.pkg.name"` |
| `name` | `string` | Core | 🔧 | — | |
| `resource_type` | `"test" \| "unit_test"` | Core | 🔧 | — | Discriminator — determines which variant shape is returned |
| `package_name` | `string \| null` | Core | 🔧 | — | |
| `description` | `string \| null` | Core | 🔧 | — | |
| `original_file_path` | `string \| null` | Core | 🔧 | — | Path to the `.yml` file defining the test |
| `tags` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet |
| `fqn` | `string[]` | Core | 🔧 | — | In `dbt.nodes` parquet |
| `file_path` | `string \| null` | Core | 🔧 | — | Rendered in TestView header via `applied.filePath`; in `dbt.nodes` parquet |
| `patch_path` | `string \| null` | Core | 🔧 | — | Rendered in TestView header via `applied.patchPath`; in `dbt.nodes` parquet |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob — confirm `dbt.nodes` parquet includes a `meta` column |
| `depends_on` | `EdgeRef[]` | Core | 🔧 | — | 1-hop upstream from `dbt.edges` parquet; maps to `parents` in GraphQL |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | |
| `depends_on[*].edge_type` | `string` | Core | 🔧 | — | |
| `execution_info` | `TestExecutionInfo \| null` | Core-conditional | 🔧 | `has_run_results` | `null` when `dbt build` hasn't run; present on both variants |
| `execution_info.status` | `string \| null` | Core-conditional | 🔧 | `has_run_results` | `"pass"` · `"fail"` · `"error"` · `"warn"` · `"skipped"` · `"reused"` |
| `execution_info.error` | `string \| null` | Core-conditional | 🔧 | `has_run_results` | Error message when status is `"error"`; `null` otherwise |
| `execution_info.completed_at` | `string \| null` | Core-conditional | 🔧 | `has_run_results` | ISO 8601 timestamp |
| `execution_info.execution_time` | `number \| null` | Core-conditional | 🔧 | `has_run_results` | Seconds (float) |
| `column_name` | `string \| null` | Core | 🔧 | — | **data test only** — column under test; from `dbt.test_metadata` parquet |
| `test_type` | `string \| null` | Core | 🔧 | — | **data test only** — `"generic"` · `"singular"`; from `dbt.nodes` parquet |
| `severity` | `string \| null` | Core | 🔧 | — | **data test only** — `"ERROR"` · `"WARN"`; from `dbt.test_metadata` parquet |
| `test_metadata` | `TestMetadata \| null` | Core | 🔧 | — | **data test only** — from `dbt.test_metadata` parquet |
| `test_metadata.name` | `string` | Core | 🔧 | — | **data test only** — e.g., `"not_null"`, `"unique"`, `"relationships"` |
| `test_metadata.kwargs` | `Record<string, unknown>` | Core | 🔍 | — | **data test only** — unstructured JSON; column name confirmed in parquet schema, exact serialization 🔍 |
| `raw_code` | `string \| null` | Core | 🔧 | — | **data test only** — SQL template; from `dbt.nodes` parquet |
| `compiled_code` | `string \| null` | Core | 🔍 | — | **data test only** — fully rendered SQL; confirm presence in `dbt.nodes` parquet |
| `model` | `string \| null` | Core | 🔍 | — | **unit test only** — the `ref(...)` expression identifying the model under test; in `dbt.unit_tests` parquet 🔍 |
| `given` | `UnitTestFixture[]` | Core | 🔧 | — | **unit test only** — input row fixtures; from `dbt.unit_tests` parquet |
| `given[*].input` | `string` | Core | 🔧 | — | **unit test only** — `ref(...)` or `source(...)` expression |
| `given[*].rows` | `Record<string, unknown>[]` | Core | 🔍 | — | **unit test only** — row data as parsed JSON; confirm parquet serialization format 🔍 |
| `expect` | `UnitTestExpect \| null` | Core | 🔧 | — | **unit test only** — expected output rows; from `dbt.unit_tests` parquet |
| `expect.rows` | `Record<string, unknown>[]` | Core | 🔍 | — | **unit test only** — expected row data; confirm parquet serialization format 🔍 |
| `num_given` | `number \| null` | Core | 🔍 | — | **unit test only** — count of `given` fixtures; from `dbt.unit_tests` parquet 🔍 |
| `num_given_rows` | `number \| null` | Core | 🔍 | — | **unit test only** — total input rows across all fixtures 🔍 |
| `num_expect_rows` | `number \| null` | Core | 🔍 | — | **unit test only** — expected output row count 🔍 |
| `config` | *(absent)* | — | ❌ | — | Class B: GraphQL `config` blob has no direct parquet column; individual fields (severity) promoted individually |
| `project_id` | *(absent)* | — | ❌ | — | Class B: platform metadata — not in any parquet table |
| `last_run_id` | *(absent)* | — | ❌ | — | Class B: run-system internal ID; no parquet path |
| `last_job_definition_id` | *(absent)* | — | ❌ | — | Class B: platform job system ID; no parquet path |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
// Discriminated union on resource_type — as decided in ADR-3
type TestDetail = DataTestDetail | UnitTestDetail;

// Shared fields factored here for documentation; Rust uses NodeBase struct
interface TestBase {
  unique_id: string;
  name: string;
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  tags: string[];
  fqn: string[];
  file_path: string | null;
  patch_path: string | null;
  meta: Record<string, unknown> | null;
  depends_on: EdgeRef[];
  execution_info: TestExecutionInfo | null;
}

interface DataTestDetail extends TestBase {
  resource_type: "test";
  column_name: string | null;
  test_type: string | null;
  severity: string | null;
  test_metadata: TestMetadata | null;
  raw_code: string | null;
  compiled_code: string | null;
}

interface UnitTestDetail extends TestBase {
  resource_type: "unit_test";
  model: string | null;
  given: UnitTestFixture[];
  expect: UnitTestExpect | null;
  num_given: number | null;
  num_given_rows: number | null;
  num_expect_rows: number | null;
}

interface TestExecutionInfo {
  status: string | null;
  error: string | null;
  completed_at: string | null;
  execution_time: number | null;
}

interface TestMetadata {
  name: string;
  kwargs: Record<string, unknown>;
}

interface UnitTestFixture {
  input: string;
  rows: Record<string, unknown>[];
}

interface UnitTestExpect {
  rows: Record<string, unknown>[];
}

// EdgeRef is shared with ModelDetail and SourceDetail
```

### Risk register

1. **Two parquet sources required for data test fields.** A complete data test response
   requires joining `dbt.nodes` (for `name`, `fqn`, `tags`, `raw_code`, `original_file_path`,
   `description`) with `dbt.test_metadata` (for `column_name`, `severity`, `kwargs`,
   `test_metadata.name`). The handler must perform a LEFT JOIN on `unique_id`. Verify the
   join key column name in both parquet files before implementing.

2. **Unit test fields are in a separate parquet table.** Unit test row fixtures (`given`,
   `expect`, `num_given`, `num_given_rows`, `num_expect_rows`, `model`) come from
   `dbt.unit_tests.parquet`. The handler must detect `resource_type = 'unit_test'` from
   `dbt.nodes` and JOIN against `dbt.unit_tests` only for that variant. Parquet
   serialization of `given` and `expect` as JSON strings or nested structs is unconfirmed.

3. **`compiled_code` presence in parquet is unverified for tests.** Confirm against the
   actual `dbt.nodes.parquet` schema. Test nodes may not populate that column. If absent,
   omit the field and remove from the contract rather than returning `null`.

4. **`execution_info` requires a run_results JOIN.** `dbt_rt.run_results` must be LEFT
   JOINed on `unique_id` to get `status`, `error`, `completed_at`,
   and `execution_time`. Gate the entire `execution_info` object behind `has_run_results`.

5. **`meta` JSONB presence in parquet is unverified.** Same risk as `SourceDetail` Risk #3.
   Confirm before adding to the SELECT; downgrade to ❌ Class B if absent.

6. **`kwargs` is unstructured JSON — fragile for relationship tests.** Per FEATURE-TO-ENDPOINT-MAPPING.md
   (F-14): parsing relationship test metadata requires matching `kwargs` keys (`to:`, `field:`,
   `column_name:`). This is a FE concern, not a handler concern — document so the FE team
   does not expect a structured object.

7. **Handler must route on `resource_type` from parquet, not path prefix.** The endpoint
   accepts a `unique_id` that may start with `test.` or `unit_test.`. The handler should
   read `resource_type` from the parquet row to determine which JOIN path and which response
   struct to use. Consistent with ADR-1's NodeBase pattern.

8. **`num_given`, `num_given_rows`, `num_expect_rows` may need to be derived.** If
   `dbt.unit_tests.parquet` stores serialized fixture arrays rather than pre-computed counts,
   these fields may need to be computed at query time (`array_length`) rather than read
   directly. Verify before implementing.

## Design notes — `GET /api/v1/exposures/:id`

This contract introduces no new ADR. It does, however, surface two FE-impacting decisions
the coordinator should be aware of before promoting:

1. **Class C exclusions are heavier here than on any prior resource.** Six fields the
   dbt-ui `ExposureView` GraphQL hook fetches (`autoBiProvider`, `integrationId`,
   `freshnessStatus`, `quality`, `upstreamStats`, `maxSnapshottedAt`) are flagged
   `subGraphs: ['internal']` in codex-api and have no parquet path. They are listed in
   the field reference as Class B `❌ absent` rather than Class C 412-stubs — they are
   not "Discovery public, CodexDB-only"; they are Discovery-internal *and* CodexDB-only.
   `healthIssues` and `projectId` are the same shape. The FE `ExposureView` must render
   graceful null states for the header trust signals badge, the modify-integration link,
   and the freshness chip when these fields are absent.

2. **No `referenced_by` on exposures.** Exposures are terminal leaf nodes — no resource
   refs an exposure. The handler must omit the field (not return `[]`), consistent with
   `SourceDetail`'s omission of `depends_on` and `SeedDetail`'s omission of `depends_on`.

Skip ADR promotion — both decisions follow established CC-5 and CC-2 conventions.

---

## `GET /api/v1/exposures/:id`

Powers: `ExposureView` / `ResourceDetailsPage` in dbt-ui — header card (type, maturity,
owner, link to BI tool), General tab (description, upstream parents, meta).
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/ExposureView.tsx`
GraphQL hooks: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useExposure.ts` → `src/hooks/discovery/exposure.ts` (`GetExposureByUniqueId`)

Exposures are downstream consumers of dbt artifacts — dashboards, ML applications, ad-hoc
analyses — declared in YAML so dbt can render them in the lineage graph. They are
**leaf nodes**: nothing refs an exposure, so this contract has `depends_on` but no
`referenced_by`. Exposures are **not executed by dbt**, so there is no `execution_info`,
no `columns`, no `catalog`, no `materialized`, no SQL body. They live in their own
parquet table — `dbt.exposures.parquet` — not in `dbt.nodes` (schema confirmed in
`crates/dbt-index/src/parquet.rs::ExposureRow`). All warehouse-shaped fields
(`database_name`, `schema_name`, `identifier`, `relation_name`) are intentionally omitted
because exposures have no warehouse object.

This is the **smallest** detail contract: 16 fields total (vs. 30+ for models).

### Example response

Fields marked `// 🔧` are not yet returned — they require a backend change (this endpoint
has no handler today; expect every field to be 🔧).
Fields marked `// 🔍` are parquet presence unverified — confirm schema before implementing.

```json
{
  "unique_id": "exposure.jaffle_shop.revenue_dashboard",
  "name": "revenue_dashboard",
  "resource_type": "exposure",
  "package_name": "jaffle_shop",
  "description": "Top-line revenue dashboard used by the finance team.",
  "original_file_path": "models/exposures.yml",
  "file_path": "models/exposures.yml",
  "tags": ["finance", "exec"],
  "fqn": ["jaffle_shop", "revenue_dashboard"],
  "label": "Revenue Dashboard",
  "exposure_type": "dashboard",
  "maturity": "high",
  "url": "https://bi.example.com/dashboards/revenue",
  "owner_name": "Jane Doe",
  "owner_email": "jane.doe@example.com",
  "meta": { "team": "finance" },
  "depends_on": [
    { "unique_id": "model.jaffle_shop.orders", "edge_type": "model" },
    { "unique_id": "source.jaffle_shop.raw_jaffle.orders", "edge_type": "source" }
  ],
  "created_at": 1747432300.5
}
```

This response has **no conditional sections**. Exposures have no execution surface and no
catalog surface, so no capability gates apply. `created_at` is the per-resource
"Definition updated as of …" timestamp per ADR-5 (epoch seconds, sourced from
`dbt.exposures.created_at`).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"exposure.pkg.name"` — no handler today |
| `name` | `string` | Core | 🔧 | — | From `dbt.exposures.name` |
| `resource_type` | `"exposure"` | Core | 🔧 | — | Always `"exposure"` for this endpoint |
| `package_name` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.package_name` |
| `description` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.description` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.original_file_path`; absolute-rooted YAML path |
| `file_path` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.file_path`; project-relative — `ExposureView` reads both — see Risk #1 |
| `tags` | `string[]` | Core | 🔧 | — | From `dbt.exposures.tags` (list_utf8 column) |
| `fqn` | `string[]` | Core | 🔧 | — | From `dbt.exposures.fqn` (list_utf8 column) |
| `label` | `string \| null` | Core | 🔧 | — | Display label override; from `dbt.exposures.label` |
| `exposure_type` | `string \| null` | Core | 🔧 | — | `"dashboard"` · `"notebook"` · `"analysis"` · `"ml"` · `"application"` — see Risk #2 |
| `maturity` | `string \| null` | Core | 🔧 | — | `"high"` · `"medium"` · `"low"` — see Risk #2 |
| `url` | `string \| null` | Core | 🔧 | — | Link to the upstream BI/app dashboard |
| `owner_name` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.owner_name` |
| `owner_email` | `string \| null` | Core | 🔧 | — | From `dbt.exposures.owner_email` |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSON-string column in parquet; needs `json_parse` at query time — see Risk #3 |
| `depends_on` | `EdgeRef[]` | Core | 🔧 | — | 1-hop upstream models + sources; derived from `dbt.exposures.depends_on_nodes` — see Risk #4 |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | |
| `depends_on[*].edge_type` | `string` | Core | 🔧 | — | Resolved from the dependency's `resource_type` (model/source) — see Risk #4 |
| `patch_path` | *(absent)* | — | ❌ | — | Not in `dbt.exposures.parquet` schema (only `file_path` and `original_file_path`); dbt-ui reads `patchPath` from GraphQL, but exposures are defined directly in YAML — the patch concept does not apply — see Risk #5 |
| `referenced_by` | *(absent)* | — | ❌ | — | Exposures are terminal leaf nodes; nothing refs an exposure. Omit entirely, not empty array |
| `manifest_generated_at` | *(absent)* | — | ❌ | — | Class B: environment-level field on the GraphQL `applied` wrapper, not on the exposure row; ingest timestamp lives in `dbt.exposures.ingested_at` and is internal |
| `parents[]` (Discovery shape) | *(absent)* | — | ❌ | — | dbt-ui's GraphQL `parents` field is replaced by `depends_on` (CC-1 / CC-2: snake_case, REST naming) — same data, REST shape |
| `auto_bi_provider` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']` in codex-api; auto-exposures are Platform-tier per FEATURE-TO-ENDPOINT-MAPPING.md F-18 — see Risk #6 |
| `integration_id` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']`; auto-exposure-only field; no parquet path |
| `freshness_status` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']`; aggregated from upstream source freshness — derive FE-side from `freshness` on each `depends_on` if needed |
| `quality` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']`; aggregated worst test status across ancestors — derive FE-side |
| `upstream_stats` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']`; Discovery-API aggregate |
| `max_snapshotted_at` | *(absent)* | — | ❌ | — | Class B: `subGraphs: ['internal']`; oldest snapshot timestamp across ancestors |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api (matches `ModelDetail` / `SourceDetail` Risk #7) |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific; no parquet path |
| `created_at` | `number \| null` | Core | 🔧 | — | Epoch seconds (float); from `dbt.exposures.created_at`. Per ADR-5, this is the "Definition updated as of …" timestamp surfaced to `ExposureView`. Empirically verified column present in `dbt.exposures.parquet`. |
| `execution_info` | *(absent)* | — | ❌ | — | Exposures are not executed by dbt; no `dbt_rt.run_results` row exists for an exposure. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `columns` | *(absent)* | — | ❌ | — | Exposures have no columns; they are downstream consumers |
| `materialized` | *(absent)* | — | ❌ | — | Exposures are not materialized to a warehouse object |
| `raw_code` | *(absent)* | — | ❌ | — | Exposures have no SQL body — YAML-only definition |
| `compiled_code` | *(absent)* | — | ❌ | — | Exposures have no SQL body |
| `database_name` | *(absent)* | — | ❌ | — | Exposures have no warehouse relation |
| `schema_name` | *(absent)* | — | ❌ | — | Exposures have no warehouse relation |
| `identifier` | *(absent)* | — | ❌ | — | Exposures have no warehouse relation |
| `relation_name` | *(absent)* | — | ❌ | — | Exposures have no warehouse relation |
| `access_level` | *(absent)* | — | ❌ | — | Model-only governance field |
| `group_name` | *(absent)* | — | ❌ | — | Not modeled on exposures |
| `contract_enforced` | *(absent)* | — | ❌ | — | Not applicable to exposures |
| `catalog` | *(absent)* | — | ❌ | — | Exposures have no warehouse object to catalog |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface ExposureDetail {
  unique_id: string;
  name: string;
  resource_type: "exposure";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  tags: string[];
  fqn: string[];
  label: string | null;
  exposure_type: string | null;
  maturity: string | null;
  url: string | null;
  owner_name: string | null;
  owner_email: string | null;
  meta: Record<string, unknown> | null;
  depends_on: EdgeRef[];
  created_at: number | null;   // ADR-5: per-resource "Definition updated as of …" timestamp; epoch seconds
}

// EdgeRef is shared with ModelDetail, SourceDetail, SeedDetail, SnapshotDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`file_path` vs. `original_file_path` for the header file list.** `ExposureView.tsx`
   passes both `appliedExposure?.patchPath` and `appliedExposure?.filePath` to
   `ResourceDetailsHeader` (deduplicating imported-auto paths). The `dbt.exposures.parquet`
   schema has `file_path` AND `original_file_path` — these are likely the same value for
   YAML-defined exposures, but `file_path` may be a project-relative form while
   `original_file_path` is absolute or root-anchored. Confirm semantics with the dbt-index
   team before the FE consumes both. If they are always equal, the FE should pick one and
   the handler can omit the duplicate.

2. **`exposure_type` and `maturity` enum values need verification.** dbt's manifest defines
   `exposure_type ∈ {dashboard, notebook, analysis, ml, application}` and
   `maturity ∈ {high, medium, low}` but does not validate the strings at parse time. The
   parquet column is plain `utf8` (no enum constraint). Document the expected values so
   FE engineers do not silently fall through unknown strings. `ExposureStatusTileSection.tsx`
   branches on `exposureType === 'dashboard'` — case sensitivity must be confirmed.

3. **`meta` is stored as a JSON string in `dbt.exposures.parquet`.** The parquet column is
   `[utf8] meta: Option<String>` — i.e., serialized JSON, not a parquet struct. The handler
   must `json_parse` (or DuckDB `json_extract`) at query time. Returning the raw string is
   incorrect; the contract specifies `Record<string, unknown>`. Verify the JSON shape is
   always an object (not array or primitive) before implementing.

4. **`depends_on` requires resolving `edge_type` from the dependency's resource type.**
   `dbt.exposures.depends_on_nodes` is a list of `unique_id` strings only — the resource
   type is implicit in the prefix (`model.`, `source.`, `metric.`, `seed.`). The handler
   must either (a) parse the prefix to derive `edge_type`, or (b) JOIN against
   `dbt.nodes` / `dbt.metrics` / etc. to read the canonical `resource_type`. Parsing the
   prefix is faster and matches what `ExposureRow` already encodes. `dbt.exposures.depends_on_macros`
   exists separately and is intentionally not surfaced — macros are not user-visible nodes.

5. **`patch_path` is absent from the parquet row but present in `ExposureView`'s
   GraphQL.** Exposures are defined directly in `.yml` (no separate `.sql` + patch
   structure), so dbt's manifest typically does not emit a distinct `patch_path` for them.
   `dbt.exposures.parquet` has only `file_path` and `original_file_path` — no
   `patch_path`. Document as ❌ Class B; do not chase. If a future dbt version starts
   writing `patch_path` to `dbt.exposures`, it can be added additively.

6. **The header trust-signals badge (`healthIssues`), modify-integration link
   (`integrationId`), and auto-exposure provider chip (`autoBiProvider`) will not render.**
   All three are Class B (`subGraphs: ['internal']` in codex-api per
   FEATURE-TO-ENDPOINT-MAPPING.md F-18). The dbt-ui `ExposureView` must render graceful
   null states. The auto-exposure flow (`pathIsImported` + `IMPORTED_AUTO_EXPOSURE_PATH_PREFIX`
   gating) is moot here — auto-exposures are Platform-tier and not in scope for
   dbt-docs-server.

7. **No `execution_info` capability gate.** Unlike models/seeds/snapshots/tests, exposures
   have no parquet row in `dbt_rt.run_results` — they are not executable. The contract
   intentionally omits `execution_info` rather than null-gating it on `has_run_results`.
   FE engineers should not look for a status badge here; an exposure's "health" is
   derivable from upstream node statuses only.

8. **No `Capabilities` flag additions for this endpoint.** All exposure fields are
   unconditional Core. The existing `has_run_results` / `has_catalog_stats` /
   `has_source_freshness` flags are not consulted by this handler. Document explicitly so
   the implementation PR is not blocked behind unrelated capability changes.

9. **New handler file required.** No `src/handlers/exposures.rs` exists today
   (confirmed against the worktree handler directory listing). Implementation should
   compose the shared `NodeBase` Rust struct per ADR-1's backend prerequisite, then add
   the exposure-specific fields (`label`, `exposure_type`, `maturity`, `url`,
   `owner_name`, `owner_email`). Register the route in `src/server.rs` and the type in
   `web/src/api.ts`.

## Design notes — `GET /api/v1/groups/:id`

Groups are the first **definition-only** resource type in the contract set. Every
endpoint before it (`models`, `sources`, `seeds`, `snapshots`, `tests`) returns
node-shaped data from `dbt.nodes`. Groups live in their own parquet table
(`dbt.groups`) and have no SQL body, no columns, no warehouse relation, no run
results, no catalog stats, no freshness, no lineage. The endpoint's purpose is
narrow: render the GroupView details panel plus an inline list of member models.

Two design choices worth flagging for the coordinator before promotion:

1. **`owner` is a nested object, not flattened scalars.** The Discovery API
   exposes `ownerName`, `ownerEmail`, `ownerSlack`, `ownerGithub` as four
   sibling fields. CC-2 says preserve nested shape; here the nesting does not
   exist in the upstream GraphQL response — we'd be **introducing** it. The
   upside is a cleaner type (`owner: { name, email, slack, github } | null`)
   that scales if more contact channels are added (Teams, PagerDuty, …). The
   downside is divergence from the FE engineers' Discovery API mental model.
   FEATURE-TO-ENDPOINT-MAPPING.md row 10 (Phase 3 cross-ref) recommends the
   nested shape explicitly. **Recommended: nested object.** Coordinator may
   override if FE prefers flat parity with Discovery.

2. **Inline `models[]` member list vs. sub-resource.** The dbt-ui GroupView
   renders a paginated table of member models inline on the page (currently
   client-paginated; server returns all members). Inlining keeps the page to
   one round trip but unbounded — a group with 200 members returns a 200-item
   array. This mirrors `ModelDetail` Risk #5 (`depends_on`/`referenced_by`
   unbounded). v0 keeps it inline with a `?first=` cap and `truncated: true`
   flag, deferring `GET /api/v1/groups/:id/models` to when a real pagination
   need surfaces. No new ADR required — same pattern as edges on `ModelDetail`.

3. **No new capability flag.** Groups have no run/catalog/freshness surface,
   so the existing `has_run_results` / `has_catalog_stats` / `has_source_freshness`
   flags don't apply. `meta`, `tags`, `owner.slack`, `owner.github` are all
   parquet-schema verification questions, not capability questions — gating them
   behind a flag would be CC-3 misuse (the flag pattern is for runtime-conditional
   data, not for "field exists in this parquet writer's output").

---

## `GET /api/v1/groups/:id`

Powers: `GroupView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/GroupView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/group.ts` (`GetGroupByUniqueId`)

Groups are **definition-only** — they have no SQL, no columns, no warehouse
relation, no run results, no catalog stats, no freshness, and no lineage edges.
The parquet source is `dbt.groups` (one row per group; columns `unique_id, name,
description, package_name, file_path, original_file_path, owner_name, owner_email,
config, ingested_at`). Member models are not stored on the group row — they are
discovered via `dbt.nodes WHERE group_name = :group.name` (the FK lives on the
node, not the group). `owner_slack` and `owner_github` are absent from the
top-level `dbt.groups` schema and are likely embedded inside the `config` JSONB
blob — verification is required before they ship; see Risk #1.

### Example response

Fields marked `// 🔧` are not yet returned — they require a backend change
(no group-detail handler exists today).
Fields marked `// 🔍` are parquet-unverified — confirm schema before implementing.

```json
{
  "unique_id": "group.jaffle_shop.finance",
  "name": "finance",
  "resource_type": "group",
  "package_name": "jaffle_shop",
  "description": "Finance domain — revenue, payments, billing models.",
  "original_file_path": "models/_groups.yml",
  "tags": ["finance", "core"],
  "owner": {
    "name": "Finance Data Team",
    "email": "finance-data@jaffle.example",
    "slack": "#finance-data",
    "github": "jaffle/finance-data-team"
  },
  "meta": { "domain": "finance", "tier": "gold" },
  "models": [
    {
      "unique_id": "model.jaffle_shop.orders",
      "name": "orders",
      "database_name": "prod",
      "schema_name": "dbt_prod",
      "contract_enforced": true
    },
    {
      "unique_id": "model.jaffle_shop.payments",
      "name": "payments",
      "database_name": "prod",
      "schema_name": "dbt_prod",
      "contract_enforced": false
    }
  ],
  "model_count": 2,
  "truncated": false,
  "ingested_at": "2026-05-19T08:30:00Z"
}
```

`owner` is `null` when neither `owner_name` nor `owner_email` is set on the
group definition. Individual sub-fields (`slack`, `github`) are independently
nullable.

`ingested_at` is the per-resource "Definition updated as of …" timestamp for
groups per ADR-5. Groups are the one ADR-5 resource type that lacks a `created_at`
column in parquet, so `dbt.groups.ingested_at` (ISO 8601) is the fallback.

`models[]` is capped at `?first=` (default 100). `truncated: true` signals the
client must paginate via the deferred `GET /api/v1/groups/:id/models` sub-resource
once it exists. `model_count` is the **total** member count, not the returned-array
length.

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

No handler exists today — every field is at minimum 🔧. Fields that additionally
require schema verification are marked 🔍.

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"group.pkg.name"` — primary key in `dbt.groups` |
| `name` | `string` | Core | 🔧 | — | Group name (e.g., `"finance"`) |
| `resource_type` | `"group"` | Core | 🔧 | — | Always `"group"` for this endpoint |
| `package_name` | `string \| null` | Core | 🔧 | — | From `dbt.groups.package_name` |
| `description` | `string \| null` | Core | 🔧 | — | From `dbt.groups.description` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | From `dbt.groups.original_file_path` — path to the `.yml` defining the group |
| `tags` | `string[]` | Core | 🔧 | — | Empirically confirmed: NOT a top-level column in `dbt.groups.parquet` (schema is `unique_id, name, description, package_name, file_path, original_file_path, owner_name, owner_email, config, ingested_at`). Handler must `json_extract(config, '$.tags')`, defaulting to `[]` on absence — see Risk #3 |
| `owner` | `OwnerInfo \| null` | Core | 🔧 | — | Nested object — see Design note #1; `null` when no owner fields set |
| `owner.name` | `string \| null` | Core | 🔧 | — | From `dbt.groups.owner_name` |
| `owner.email` | `string \| null` | Core | 🔧 | — | From `dbt.groups.owner_email` |
| `owner.slack` | `string \| null` | Core | 🔧 | — | Empirically confirmed absent at the top level (only `owner_name` and `owner_email` are dedicated columns). Handler must `json_extract_string(config, '$.owner.slack')` if present; emit `null` otherwise — see Risk #1 |
| `owner.github` | `string \| null` | Core | 🔧 | — | Empirically confirmed absent at the top level. Same `json_extract` path on `config` as `owner.slack` — see Risk #1 |
| `meta` | `Record<string, unknown> \| null` | Core | 🔧 | — | Empirically confirmed absent at the top level. Handler must `json_extract(config, '$.meta')` and parse as JSON object; default to `null` on absence — see Risk #3 |
| `models` | `GroupMember[]` | Core | 🔧 | — | Member models from `dbt.nodes WHERE group_name = :name AND resource_type = 'model'`; capped by `?first=` — see Risk #2 |
| `models[*].unique_id` | `string` | Core | 🔧 | — | From `dbt.nodes.unique_id` |
| `models[*].name` | `string` | Core | 🔧 | — | From `dbt.nodes.name` |
| `models[*].database_name` | `string \| null` | Core | 🔧 | — | From `dbt.nodes.database_name` |
| `models[*].schema_name` | `string \| null` | Core | 🔧 | — | From `dbt.nodes.schema_name` |
| `models[*].contract_enforced` | `boolean \| null` | Core | 🔧 | — | From `dbt.nodes.contract_enforced`; `null` if unset (mirrors `ModelDetail`) |
| `model_count` | `number` | Core | 🔧 | — | Total count of member models — unaffected by `?first=` truncation |
| `truncated` | `boolean` | Core | 🔧 | — | `true` if `model_count > models.length`; prompts deferred sub-resource |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud concept; not in local parquet (Discovery `projectId` is the multi-env Cloud project ID) |
| `last_updated_at` | *(absent)* | — | ❌ | — | Class B: Cloud-managed environment timestamp; not in `dbt.groups` (the parquet has `ingested_at` which is server-local, not semantically equivalent) |
| `file_path` | *(absent)* | — | ❌ | — | Internal compiled path; `original_file_path` covers the UI use case |
| `models[*].materialized` | *(absent)* | — | ❌ | — | Out of scope for the inline summary; consumers wanting it call `GET /api/v1/models/:id` |
| `models[*].description` | *(absent)* | — | ❌ | — | Same — kept off the summary row to bound payload size |
| `depends_on` | *(absent)* | — | ❌ | — | Groups have no upstream dependencies; omit entirely (not empty array) — mirrors `SourceDetail` convention |
| `referenced_by` | *(absent)* | — | ❌ | — | The "referenced_by" relationship for a group is its `models[]` member list; do not duplicate as edges |
| `columns` | *(absent)* | — | ❌ | — | Groups are definition-only; no columns |
| `raw_code` / `compiled_code` | *(absent)* | — | ❌ | — | Groups have no SQL body |
| `materialized` / `relation_name` / `database_name` / `schema_name` / `identifier` | *(absent)* | — | ❌ | — | Groups have no warehouse relation |
| `access_level` / `group_name` / `contract_enforced` | *(absent)* | — | ❌ | — | Model-level config; not applicable to groups (a group does not belong to a group) |
| `ingested_at` | `string \| null` | Core | 🔧 | — | ISO 8601 timestamp; from `dbt.groups.ingested_at` (the most recent index write that touched this row). Per ADR-5, this is the "Definition updated as of …" timestamp for groups; **groups are the one ADR-5 resource that has no `created_at` column** in parquet (verified against the sample project schema), so `ingested_at` is the fallback. If `dbt-index` adds a `created_at` column to `dbt.groups` later, flip to `created_at` like the other 5 endpoints. |
| `execution_info` | *(absent)* | — | ❌ | — | Groups never run — definition-only. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `catalog` | *(absent)* | — | ❌ | — | No warehouse relation; nothing to catalog |
| `freshness` | *(absent)* | — | ❌ | — | No source semantics |
| `fqn` | *(absent)* | — | ❌ | — | Not in `dbt.groups` parquet schema; groups are identified by `unique_id` alone |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |
| `patch_path` | *(absent)* | — | ❌ | — | Class B: YAML-only resource — `original_file_path` IS the `.yml` file containing the group definition; the patch concept does not apply (a "patch" is a separate YAML that augments a non-YAML primary definition, e.g. `.sql` + `schema.yml`). Discovery's `patchPath` would be null or duplicate `originalFilePath` for this resource. |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface GroupDetail {
  unique_id: string;
  name: string;
  resource_type: "group";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  tags: string[];
  owner: OwnerInfo | null;
  meta: Record<string, unknown> | null;
  models: GroupMember[];
  model_count: number;
  truncated: boolean;
  ingested_at: string | null;  // ADR-5: groups have no `created_at` column in parquet — `ingested_at` (ISO 8601) is the fallback
}

interface OwnerInfo {
  name: string | null;
  email: string | null;
  slack: string | null;
  github: string | null;
}

interface GroupMember {
  unique_id: string;
  name: string;
  database_name: string | null;
  schema_name: string | null;
  contract_enforced: boolean | null;
}
```

### Risk register

1. **`owner.slack` and `owner.github` absent at top level — [RESOLVED].**
   Empirically confirmed against `sl-schema-evolution/sample_project/target/index/dbt.groups.parquet`:
   the schema has only `owner_name` and `owner_email` as dedicated owner columns.
   Slack/GitHub handles, if present, live inside the `config` JSON column.
   Handler extracts via `json_extract_string(config, '$.owner.slack')` /
   `'$.owner.github'`. If the JSON doesn't contain them, fields ship as `null`.
   Frontend renders gracefully either way. **Decision: ship Core-stable with
   these documented JSON paths as the starting recommendation; if a real project
   populates these fields under a different JSON path, fix forward in a follow-up
   patch.** The sample project's 2 groups have `null` for both, so we can't
   round-trip-verify against this corpus; the integration test materializes when
   a real project surfaces the data. No dbt-index schema change required.

2. **`models[]` is unbounded without a cap.** A group with 200 member models
   returns a 200-row array. Same problem as `ModelDetail` Risk #5 for edges.
   Mitigation for v0: accept `?first=` (default 100, max 500), report `model_count`
   as the total, and set `truncated: true` when truncated. Cursor pagination via
   `GET /api/v1/groups/:id/models` is deferred until a real pagination need
   surfaces (no v0 UI consumer hits the cap — typical groups have <50 members).

3. **`tags` and `meta` parquet provenance — [RESOLVED].** Empirically
   confirmed against the sample project: neither `tags` nor `meta` is a top-level
   column in `dbt.groups.parquet`. Both must be sourced from the `config` JSON
   column (`json_extract(config, '$.tags')` / `'$.meta'`). If the JSON omits them,
   ship `[]` / `null` respectively. **Decision: same posture as Risk #1 above
   — ship Core-stable; documented paths are the starting recommendation; fix
   forward if real-project data surfaces a different JSON shape.** Sample-project
   data has empty `config` for both in its 2 rows; the integration test materializes
   when a real project surfaces the data. No dbt-index schema change required.

4. **Member-model query joins on `(package_name, name)`, not `unique_id` — [DECIDED].**
   `dbt.nodes.group_name` stores the group **name** (e.g., `"finance"`), not the full
   `unique_id` (`"group.jaffle_shop.finance"`). Decision: the handler scopes the JOIN
   by package as well as name to prevent cross-package collisions:
   `SELECT n.* FROM dbt.nodes n JOIN dbt.groups g ON n.group_name = g.name
   AND n.package_name = g.package_name WHERE g.unique_id = :id`. This matches
   dbt-core's group resolution (a group is local to its package). Verified safe
   for the two-row sample project; revisit if a multi-package project surfaces a
   case where this is wrong.

5. **No `execution_info` despite groups appearing in run-result contexts.**
   `dbt build` emits run results for member models, not for the group itself.
   The GroupView component has commented-out `updatedAt` logic — confirmed
   intentional (the Discovery API returns `lastUpdatedAt` on the parent
   `environment.definition`, not on the group). Do not add `execution_info`;
   any "last updated" surface belongs on `/api/v1/project` or a future
   environment-level endpoint, not here.

6. **`resource_type` value choice — singular vs. discriminator parity.**
   The unique_id prefix is `group.` (singular). Other resource types use the
   prefix verbatim (`model`, `source`, `seed`, `snapshot`). Choose `"group"`
   to match the prefix and the icon parity table (`group → RyeconGroup`). The
   list-endpoint surface (when `GET /api/v1/groups` lands) should also serialize
   the type as `"group"`, not `"groups"`.

7. **Definition-only resources may need `NodeBase` to be relaxed.** Per ADR-1's
   backend prerequisite, all typed detail handlers compose a shared `NodeBase`
   struct. `NodeBase` currently includes `fqn: Vec<String>` (required) — but
   groups have no `fqn` in `dbt.groups` parquet. Implementer must either
   (a) make `fqn` `Option<Vec<String>>` on `NodeBase`, (b) synthesize a 2-element
   `[package_name, name]` for groups, or (c) accept that group handler diverges
   from `NodeBase`. Decide before the generic dispatcher lands (ADR-1 deferred
   item) since the dispatcher assumes all typed handlers compose `NodeBase`.

## Design notes — `GET /api/v1/macros/:id`

Macros are the first **definition-only** resource type in this contract series — they have
no `dbt_rt.run_results` entry, no warehouse relation, no catalog stats, and no columns.
This means the contract excludes every Core-conditional surface (`execution_info`,
`catalog`, `freshness`) and every column-related field. The response is materially smaller
than `ModelDetail` / `SourceDetail` / `SeedDetail`, with no new capability flags introduced.

Two contract decisions worth flagging for coordinator review (neither rises to a full ADR
since both follow precedent set by ADR-1/ADR-2 and existing contracts):

1. **`arguments[]` is inlined, not promoted to a sub-resource.** Mirrors how `columns[]`
   is inlined on `ModelDetail`. The argument count per macro is bounded by author practice
   (typically <10), so pagination is not a concern. Shape preserved verbatim from the
   GraphQL `MacroArgument` type: `{ name, description, type }`.

2. **`depends_on` and `referenced_by` are inlined as `MacroEdgeRef[]` despite not flowing
   through `dbt.edges`.** The `dbt.edges` table is `edge_type: "ref"` only and ignores
   macro relationships entirely. Both edge sets are derivable from parquet:
   - `depends_on` from this macro's own `dbt.macros.depends_on_macros` list column;
   - `referenced_by` from inverse scans of `dbt.nodes.depends_on_macros` (and other
     resource tables that carry a `depends_on_macros` column — exposures, metrics,
     saved_queries, semantic_models, unit_tests).

   This is a handler implementation detail, not a contract change. The wire shape matches
   the existing `EdgeRef` type used by `ModelDetail` / `SourceDetail`. No new capability
   flag is needed because both are pure-parquet derivations available in Core (`dbt parse`
   suffices).

---

## `GET /api/v1/macros/:id`

Powers: `MacroView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/MacroView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/macro.ts` (`GetMacroByUniqueId`)

Macros are Jinja templates compiled into nodes at parse time — pure definition-only
resources with no warehouse representation. They live in their own parquet table
(`dbt.macros`, **not** `dbt.nodes`), which is why the handler cannot share the existing
`nodes.rs` SELECT and must own its own query. As a definition-only resource, `MacroDetail`
has **no** `execution_info`, **no** `columns`, **no** `catalog`, **no** `materialized`,
**no** `relation_name`, **no** `freshness`, and **no** `database_name` / `schema_name` —
macros never land in a warehouse. The detail page renders three tabs: General (description,
metadata, relationships), Arguments (the inlined `arguments[]`), and Code (the raw
`macro_sql`).

### Example response

Fields marked `// 🔧` are not yet returned — no handler exists today; everything is 🔧.
Fields marked `// 🔍` are parquet-unverified — confirm schema before implementing.

```json
{
  "unique_id": "macro.jaffle_shop.cents_to_dollars",
  "name": "cents_to_dollars",
  "resource_type": "macro",
  "package_name": "jaffle_shop",
  "description": "Convert an integer cents column to a dollar-denominated decimal.",
  "original_file_path": "macros/cents_to_dollars.sql",
  "file_path": "macros/cents_to_dollars.sql",
  "patch_path": "macros/schema.yml",
  "macro_sql": "{% macro cents_to_dollars(column_name, scale=2) -%}\n  ({{ column_name }} / 100)::numeric(16, {{ scale }})\n{%- endmacro %}",
  "meta": { "owner": "data-eng" },
  "docs_show": true,
  "supported_languages": ["sql"],
  "arguments": [
    {
      "name": "column_name",
      "type": "string",
      "description": "The integer column holding cent values."
    },
    {
      "name": "scale",
      "type": "integer",
      "description": "Decimal scale to round the output to."
    }
  ],
  "depends_on": [
    { "unique_id": "macro.dbt.type_numeric", "edge_type": "macro" }
  ],
  "referenced_by": [
    { "unique_id": "model.jaffle_shop.orders", "edge_type": "macro" },
    { "unique_id": "model.jaffle_shop.payments", "edge_type": "macro" }
  ],
  "created_at": 1746000000.0
}
```

No capability gates apply to this response — every field is either Core (parquet-backed and
unconditional) or a Class B exclusion. No `execution_info`, `catalog`, or `freshness` block
exists for macros. `created_at` is the per-resource "Definition updated as of …" timestamp
per ADR-5 (epoch seconds, sourced from `dbt.macros.created_at`).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

No handler exists for `GET /api/v1/macros/:id` today; every included field is 🔧 (or 🔍).

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"macro.pkg.name"` — from `dbt.macros.unique_id` |
| `name` | `string` | Core | 🔧 | — | From `dbt.macros.name` |
| `resource_type` | `"macro"` | Core | 🔧 | — | Always `"macro"` for this endpoint; constant in handler — not a parquet column |
| `package_name` | `string \| null` | Core | 🔧 | — | From `dbt.macros.package_name` |
| `description` | `string \| null` | Core | 🔧 | — | From `dbt.macros.description` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | From `dbt.macros.original_file_path`; relative to project root |
| `file_path` | `string \| null` | Core | 🔧 | — | From `dbt.macros.file_path`; relative to project root |
| `patch_path` | `string \| null` | Core | 🔧 | — | From `dbt.macros.patch_path`; YAML schema file declaring the macro's arguments |
| `macro_sql` | `string \| null` | Core | 🔧 | — | From `dbt.macros.macro_sql`; the Jinja template source |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSON blob — `dbt.macros.meta` is declared `Option<String>` (serialized JSON); confirm round-trip parses cleanly. Same risk class as `meta` on `SeedDetail` / `SourceDetail` |
| `docs_show` | `boolean` | Core | 🔧 | — | From `dbt.macros.docs_show`; whether the macro should appear in generated docs. FE may use this to hide internal helpers — currently the dbt-ui MacroView does not gate on it, but the value is cheap to expose |
| `supported_languages` | `string[]` | Core | 🔧 | — | From `dbt.macros.supported_languages`; e.g., `["sql"]`, `["python"]`. Empty array if unset |
| `arguments` | `MacroArgument[]` | Core | 🔍 | — | From `dbt.macros.arguments` (stored as JSON string — `Option<String>`). Handler must `json_extract` and re-serialize as a list of `{name, type, description}` objects. Empty array if no declared arguments |
| `arguments[*].name` | `string` | Core | 🔍 | — | Required field on each argument |
| `arguments[*].type` | `string \| null` | Core | 🔍 | — | Declared argument type (e.g., `"string"`, `"integer"`); free-form Jinja convention, not validated |
| `arguments[*].description` | `string \| null` | Core | 🔍 | — | Per-argument description from YAML schema patch |
| `depends_on` | `MacroEdgeRef[]` | Core | 🔧 | — | Upstream macros this macro calls. Derived from `dbt.macros.depends_on_macros` (list column). Each entry's `edge_type` is `"macro"`. Empty array if the macro depends on no other macros |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | e.g., `"macro.dbt.type_numeric"` |
| `depends_on[*].edge_type` | `"macro"` | Core | 🔧 | — | Always `"macro"` for macro-to-macro edges |
| `referenced_by` | `MacroEdgeRef[]` | Core | 🔧 | — | Downstream resources that invoke this macro. Derived by scanning every parquet table that carries a `depends_on_macros` list column (`dbt.nodes`, `dbt.exposures`, `dbt.metrics`, `dbt.saved_queries`, `dbt.semantic_models`, `dbt.unit_tests`, and `dbt.macros` itself) for entries containing this macro's `unique_id`. See Risk #2 |
| `referenced_by[*].unique_id` | `string` | Core | 🔧 | — | |
| `referenced_by[*].edge_type` | `"macro"` | Core | 🔧 | — | Always `"macro"` — this is a Jinja-call relationship, not a SQL `ref()` |
| `tags` | *(absent)* | — | ❌ | — | Class B for macros: `dbt.macros` parquet has no `tags` column. GraphQL exposes `tags` but it is sourced from manifest-only metadata that codex-api persists separately — no parquet path. Document explicitly so FE engineers don't chase it. See Risk #3 |
| `fqn` | *(absent)* | — | ❌ | — | Class B for macros: `dbt.macros` parquet has no `fqn` column (unlike `dbt.nodes`). dbt manifests do not assign an FQN to macros; their identity is `package.macro_name` |
| `run_id` | *(absent)* | — | ❌ | — | Class B: Cloud invocation ID; not in local parquet. GraphQL exposes `runId` but it's a CodexDB-only concept |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud project ID; not in local parquet. GraphQL exposes `projectId` but it's a CodexDB-only concept |
| `created_at` | `number \| null` | Core | 🔧 | — | Epoch seconds (float); from `dbt.macros.created_at`. Per ADR-5 the field is exposed on every ADR-5–scoped detail endpoint for consistency. No current dbt-ui consumer renders this for macros (`MacroView` shows `updatedAt = undefined`), but the data is free to surface and a future UI consumer can pick it up without a wire-format break. Empirically verified column present in `dbt.macros.parquet` across 671 rows. |
| `execution_info` | *(absent)* | — | ❌ | — | Macros are not runnable — `dbt_rt.run_results` does not track macro executions. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `columns` | *(absent)* | — | ❌ | — | Macros are templates; they have no warehouse columns |
| `catalog` | *(absent)* | — | ❌ | — | Macros have no warehouse relation; no catalog stats apply |
| `materialized` | *(absent)* | — | ❌ | — | Not applicable to macros |
| `database_name` / `schema_name` / `identifier` / `relation_name` | *(absent)* | — | ❌ | — | Macros do not land in a warehouse |
| `raw_code` | *(absent)* | — | ❌ | — | Macro template source is in `macro_sql`; there is no separate `raw_code` field on `dbt.macros` |
| `compiled_code` | *(absent)* | — | ❌ | — | Macros are not compiled standalone; they are inlined into other nodes' compiled SQL |
| `access_level` / `group_name` / `contract_enforced` | *(absent)* | — | ❌ | — | Not applicable to macros |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api — consistent with `ModelDetail` precedent |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface MacroDetail {
  unique_id: string;
  name: string;
  resource_type: "macro";
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  patch_path: string | null;
  macro_sql: string | null;
  meta: Record<string, unknown> | null;
  docs_show: boolean;
  supported_languages: string[];
  arguments: MacroArgument[];
  depends_on: MacroEdgeRef[];
  referenced_by: MacroEdgeRef[];
  created_at: number | null;   // ADR-5: per-resource "Definition updated as of …" timestamp; epoch seconds
}

interface MacroArgument {
  name: string;
  type: string | null;
  description: string | null;
}

// MacroEdgeRef is structurally identical to EdgeRef but with a narrower edge_type domain
interface MacroEdgeRef {
  unique_id: string;
  edge_type: "macro";
}
```

### Risk register

1. **No handler exists yet; SELECT must target `dbt.macros`, not `dbt.nodes`.** Every other
   typed detail endpoint to date (`models`, `sources`, `seeds`, `snapshots`, `tests`) reads
   primarily from `dbt.nodes`. Macros live in `dbt.macros`, a fully separate parquet table
   with its own column set. The `nodes.rs` query layout does not transfer — this endpoint
   needs its own handler file (`src/handlers/macros.rs`) with its own SELECT. `NodeBase`
   (ADR-1 backend prerequisite) still composes cleanly: `unique_id`, `name`,
   `resource_type`, `package_name`, `description`, `original_file_path`. `tags` and `fqn`
   from `NodeBase` are omitted on the wire for macros — the Rust struct will need
   per-resource-type filtering of `NodeBase` fields during serialization, OR `MacroDetail`
   declines to compose `NodeBase` and duplicates the six common columns. Decide before
   implementation.

2. **`referenced_by` requires a fan-out scan across six parquet tables — [DECIDED for v0: accept fan-out].** Macro `referenced_by` must scan `depends_on_macros` list columns on `dbt.nodes`, `dbt.exposures`, `dbt.metrics`, `dbt.saved_queries`, `dbt.semantic_models`, `dbt.unit_tests`, and `dbt.macros` itself. DuckDB's `list_contains` over a list-typed column is supported; cost is O(rows) per table per request. Decision: ship v0 with the fan-out; profile the macros endpoint against the 671-row sample project after merge. If a popular utility macro (e.g., a project-wide `dbt_utils.*` wrapper) is observed dominating request time, the follow-up is a one-time inverted-index build at server boot (`materialize macro_edges as SELECT macro_unique_id, referrer_unique_id FROM …`); the index is immutable during a server's lifetime so the build cost is paid once.

3. **`tags` is fetched by GraphQL but has no parquet path — [RESOLVED].**
   Empirically confirmed against `dbt.macros.parquet` (671 rows in the sample project):
   schema is `unique_id, name, package_name, file_path, original_file_path, macro_sql, description, depends_on_macros, arguments, docs_show, patch_path, supported_languages, meta, created_at, ingested_at` — no `tags` column. The FE must render a graceful absent state.
   Document explicitly so FE engineers don't add a `tags?` optional to the type and silently
   render an empty tag list as "no tags" when it should be "tags unavailable in this
   build." Treating as ❌ Class B with no upgrade path is the correct call.

4. **`meta` JSONB parsing is unverified.** Same parquet-storage shape as on `SeedDetail` /
   `SourceDetail` (`meta` stored as `Option<String>` JSON). Confirm DuckDB's
   `json_extract` / `json_object` round-trip cleanly into a `serde_json::Value` for the
   response. Resolved together with the same risk on the seeds/sources contracts.

5. **`arguments` JSON shape is parquet-stored, not first-class.** `dbt.macros.arguments`
   is `Option<String>` containing a serialized JSON array. The handler must parse it once
   per row, validate each entry has at least a `name` field, and re-emit as
   `MacroArgument[]`. Malformed entries (missing `name`) should be filtered out, not error
   the request — log a warning. Confirm the JSON shape against a real ingested macro before
   committing to the typed `MacroArgument` interface — if there are additional fields in
   the JSON (e.g., `default`), decide whether to surface them or strictly project to
   `{name, type, description}`.

6. **`depends_on` cardinality is unbounded but practically small.** Macros that call many
   other macros are rare; a v0 implementation can omit a `?first=` cap. If a pathological
   macro emerges (50+ upstream calls), promote to the same `truncated` + pagination story
   documented in `ModelDetail` Risk #5.

7. **`runId` / `projectId` deliberately dropped from response.** Both are CodexDB-specific
   identifiers with no analog in stateless docs. Avoid the temptation to "stub them as
   `null`" — that would imply they could one day be populated locally, which they cannot.
   Document explicitly as Class B so a future engineer doesn't try to wire them up.

8. **`MacroEdgeRef.edge_type` is a singleton constant.** Every entry in both `depends_on`
   and `referenced_by` has `edge_type: "macro"`. The literal-typed `"macro"` in the
   TypeScript interface signals this to FE engineers and forecloses on confusion with model
   `"ref"` edges. The Rust handler should emit the string literal, not derive it from
   parquet (there is no edge_type column for macro relationships).

## Design notes — `GET /api/v1/metrics/:id`

The following observations did not warrant a full ADR but should inform the integrated
contract. Promote any of these to an ADR only if the coordinator decides the question is
load-bearing for v0 implementation.

1. **No `execution_info` on metrics.** Metrics are Semantic Layer definitions, not
   warehouse-materialized objects. `dbt build` does not "run" a metric in a way that
   produces a `dbt_rt.run_results` row keyed on a `metric.*` `unique_id` — Discovery API
   reflects this (the `MetricDefinitionNode` GraphQL type has no `executionInfo`,
   `lastRunStatus`, or `lastRunError` fields). The contract omits `execution_info`
   entirely (not `null`-gated). MetricView in dbt-ui confirms this: it has only a
   `general` tab with no run-status badge.

2. **No `catalog` on metrics.** Metrics are not warehouse relations. No `dbt.catalog_tables`
   row exists for a metric `unique_id`. Omit entirely.

3. **No `columns` on metrics.** Columns are a property of relations (models, sources,
   seeds, snapshots). Metrics expose `measures`, `dimensions`, and `time_granularity`
   instead — these live on the underlying `semantic_model`, not on the metric itself in
   parquet. The dbt-ui MetricView does not render a Columns tab.

4. **`type_params` is a JSON blob, not a discriminated union.** The `dbt.metrics.parquet`
   schema stores `type_params` as an opaque JSON string (`Option<String>`, serialized via
   `jjson(m, "type_params")` in `build_metric_row`). The shape varies by metric `type`
   (`simple` uses `measure`; `ratio` uses `numerator`/`denominator`; `derived` uses
   `metrics[]` + `expr`; `cumulative` uses `window` + `grain_to_date`). This contract
   returns `type_params` as `Record<string, unknown>` and **does not** introduce a
   discriminated union on the Rust side — the front end already handles the variants
   via Zod (`zTypeParams` in the dbt-ui hook). Promoting to a discriminated union would
   require parsing JSON in the handler and would double the response-type surface area
   without a current UI consumer asking for it.

5. **`formula` is fetched by the GraphQL hook but absent from `dbt.metrics.parquet` — [RESOLVED].**
   The hook selects `formula`, and the introspected GraphQL type exposes it
   (`MetricDefinitionNode.formula: Maybe<String>`). `MetricRow` in
   `crates/dbt-index/src/parquet.rs` has no `formula` column — only `metric_filter`
   and `type_params`. Empirically confirmed against the sample project: for `derived`
   metrics the expression lives at `type_params.expr` (observed:
   `"total_enrollments / total_classes_enrolled"`). The contract classifies `formula`
   as ❌ Class B; FE reads `type_params.expr` directly for derived metrics. No dbt-index
   schema change required — see Risk #3.

6. **`runGeneratedAt` header timestamp — [RESOLVED via `created_at`].** MetricView
   renders "Definition updated as of …" in the header using
   `metric.definition.runGeneratedAt`. Prior framing claimed the parquet had no
   per-metric timestamp; empirically refuted — `dbt.metrics.parquet` has both
   `created_at: double` (epoch seconds) and `ingested_at: timestamp[us, tz=UTC]`.
   Per ADR-5, the contract surfaces `created_at` as the per-resource "Definition
   updated as of …" timestamp. `run_generated_at` itself remains ❌ Class B in the
   field reference (the Cloud-API name has no parquet analogue), but the FE no
   longer needs to fall back to project-level metadata. See Risk #7.

7. **No new capability flag introduced.** None of the metric fields require a new flag.
   Existing flags (`has_run_results`, `has_catalog_stats`, `has_source_freshness`) are
   not applicable — metrics have no execution, no catalog, no freshness.

---

## `GET /api/v1/metrics/:id`

Powers: `MetricView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/MetricView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useMetric.ts` → `src/hooks/discovery/metric.ts` (`GetMetricByUniqueId`)

Metrics are Semantic Layer (MetricFlow) definitions: business-logic aggregations declared
in YAML and resolved at query time, not materialized as warehouse objects. Their parquet
home is `dbt.metrics.parquet` (`MetricRow` in `crates/dbt-index/src/parquet.rs`), which is
written by `dbt --use-index` parsing of the manifest. Each metric has a `type` discriminator
(`simple`, `ratio`, `derived`, `cumulative`) whose meaning is carried by the `type_params`
JSON blob — this contract preserves that shape rather than imposing a Rust-side discriminated
union (see Design note 4). Metrics have **no `execution_info`, `catalog`, or `columns`** —
those concepts do not apply to Semantic Layer definitions (see Design notes 1–3).

### Example response

Fields marked `// 🔧` are not yet returned — they require a backend change.
Fields marked `// 🔍` are parquet-unverified — confirm schema before implementing.

```json
{
  "unique_id": "metric.jaffle_shop.total_revenue",
  "name": "total_revenue",
  "resource_type": "metric",
  "package_name": "jaffle_shop",
  "label": "Total revenue",
  "description": "Sum of order amounts across all completed orders.",
  "original_file_path": "models/marts/metrics.yml",
  "file_path": "models/marts/metrics.yml",
  "fqn": ["jaffle_shop", "total_revenue"],
  "tags": ["finance"],
  "metric_type": "simple",
  "type_params": {
    "measure": { "name": "order_amount", "alias": null, "filter": null },
    "input_measures": [
      { "name": "order_amount", "alias": null, "filter": null }
    ]
  },
  "filter": {
    "where_filters": [
      { "where_sql_template": "{{ Dimension('orders__status') }} = 'completed'" }
    ]
  },
  "time_granularity": "day",
  "semantic_model_name": "orders",
  "input_metric_names": [],
  "group_name": "finance",
  "meta": { "owner": "data-eng" },
  "depends_on": [
    { "unique_id": "semantic_model.jaffle_shop.orders", "edge_type": "semantic_model" }
  ],
  "referenced_by": [
    { "unique_id": "saved_query.jaffle_shop.weekly_revenue", "edge_type": "saved_query" }
  ],
  "created_at": 1747432300.5
}
```

`created_at` is the per-resource "Definition updated as of …" timestamp per ADR-5
(epoch seconds, sourced from `dbt.metrics.created_at`).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"metric.pkg.name"` — from `dbt.metrics.unique_id` |
| `name` | `string` | Core | 🔧 | — | From `dbt.metrics.name` |
| `resource_type` | `"metric"` | Core | 🔧 | — | Always `"metric"` for this endpoint |
| `package_name` | `string \| null` | Core | 🔧 | — | From `dbt.metrics.package_name` |
| `label` | `string \| null` | Core | 🔧 | — | Human-readable name; from `dbt.metrics.label` |
| `description` | `string \| null` | Core | 🔧 | — | From `dbt.metrics.description` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | From `dbt.metrics.original_file_path`; path to the YAML file relative to project root |
| `file_path` | `string \| null` | Core | 🔧 | — | From `dbt.metrics.file_path`; rendered model-relative path; powers MetricView header file link |
| `fqn` | `string[]` | Core | 🔧 | — | From `dbt.metrics.fqn`; rendered in GeneralView LineageSection |
| `tags` | `string[]` | Core | 🔧 | — | From `dbt.metrics.tags` |
| `metric_type` | `string \| null` | Core | 🔧 | — | `"simple"` · `"ratio"` · `"derived"` · `"cumulative"` · `"conversion"`; from `dbt.metrics.metric_type` (= manifest `type`). Discriminator for `type_params` shape — see Design note 4. Empirically verified against `dbt.metrics.parquet` in `sl-schema-evolution/sample_project` (all 5 values observed). |
| `type_params` | `Record<string, unknown> \| null` | Core | 🔧 | — | Variant-shaped per `metric_type`; from `dbt.metrics.type_params` JSON column — deserialize the stored JSON string into a JSON object. Shape mirrors manifest v10 `metrics[].type_params` |
| `filter` | `Record<string, unknown> \| null` | Core | 🔧 | — | Where-filter object; from `dbt.metrics.metric_filter` JSON column. Discovery GraphQL exposes this as untyped `JSONObject`; the dbt-ui renderer reads `filter.where_filters[].where_sql_template`. Preserve the manifest shape; do not flatten |
| `time_granularity` | `string \| null` | Core | 🔧 | — | `"day"` · `"week"` · `"month"` · `"quarter"` · `"year"`; from `dbt.metrics.time_granularity` |
| `semantic_model_name` | `string \| null` | Core | 🔧 | — | Denormalized from `type_params.metric_aggregation_params.semantic_model`; from `dbt.metrics.semantic_model_name` |
| `input_metric_names` | `string[]` | Core | 🔧 | — | Names of input metrics for `ratio` (numerator/denominator) and `derived` (metrics[]) types; from `dbt.metrics.input_metric_names` (denormalized in `build_metric_row`) |
| `group_name` | `string \| null` | Core | 🔧 | — | From `dbt.metrics.group_name` (= manifest `group`) |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | JSONB blob; `dbt.metrics.meta` is `Option<String>` in `MetricRow` (JSON-serialized). Confirm DuckDB JSON parsing is wired before exposing as object vs. raw string |
| `depends_on` | `EdgeRef[]` | Core | 🔧 | — | 1-hop upstream from `dbt.edges` parquet; typically points to a `semantic_model.*` (for `simple`/`cumulative`) or `metric.*` entries (for `ratio`/`derived`). Maps to `parents` in GraphQL |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | |
| `depends_on[*].edge_type` | `string` | Core | 🔧 | — | e.g., `"semantic_model"`, `"metric"` |
| `referenced_by` | `EdgeRef[]` | Core | 🔧 | — | 1-hop downstream; typically `saved_query.*` or downstream `metric.*` (derived/ratio consumers). Maps to `children` in GraphQL |
| `referenced_by[*].unique_id` | `string` | Core | 🔧 | — | |
| `referenced_by[*].edge_type` | `string` | Core | 🔧 | — | |
| `formula` | *(absent)* | — | ❌ | — | Class B: not in `dbt.metrics.parquet`. For `derived` metrics, the expression lives in `type_params.expr` — FE should read it there. See Design note 5 |
| `run_generated_at` | *(absent)* | — | ❌ | — | Class B: Discovery's `runGeneratedAt` is a Cloud manifest-snapshot timestamp without a parquet analogue under that name. The "Definition updated as of …" header is served by the per-resource `created_at` row above (per ADR-5); the FE consumes `created_at`, not `run_generated_at`. See Design note 6 |
| `patch_path` | *(absent)* | — | ❌ | — | Class B: `MetricRow` has no `patch_path` column (unlike `NodeRow`/`MacroRow`). Metrics are defined directly in YAML; `original_file_path` is the YAML file. The MetricView header file-link logic falls back to `filePath` |
| `created_at` | `number \| null` | Core | 🔧 | — | Epoch seconds (float); from `dbt.metrics.created_at`. Per ADR-5, this is the "Definition updated as of …" timestamp surfaced to `MetricView`. Empirically verified column present in `dbt.metrics.parquet` across 43 rows in the sample project. |
| `execution_info` | *(absent)* | — | ❌ | — | Metrics do not execute in the warehouse sense; no `dbt_rt.run_results` row keyed on `metric.*`. See Design note 1. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `catalog` | *(absent)* | — | ❌ | — | Metrics are not warehouse relations; no `dbt.catalog_tables` row. See Design note 2 |
| `columns` | *(absent)* | — | ❌ | — | Metrics expose measures/dimensions/granularity via `type_params` and the upstream `semantic_model`, not columns. See Design note 3 |
| `materialized` | *(absent)* | — | ❌ | — | Not applicable; metrics are not materialized |
| `relation_name` | *(absent)* | — | ❌ | — | Not applicable; metrics are not warehouse objects |
| `database_name` | *(absent)* | — | ❌ | — | Not applicable; same reason as `relation_name` |
| `schema_name` | *(absent)* | — | ❌ | — | Not applicable; same reason as `relation_name` |
| `identifier` | *(absent)* | — | ❌ | — | Not applicable; same reason as `relation_name` |
| `access_level` | *(absent)* | — | ❌ | — | Model-only governance field; not applicable to metrics |
| `contract_enforced` | *(absent)* | — | ❌ | — | Model-only governance field; not applicable to metrics |
| `raw_code` | *(absent)* | — | ❌ | — | Metrics have no SQL body; closest is `type_params.expr` for derived metrics |
| `compiled_code` | *(absent)* | — | ❌ | — | Metrics have no SQL body |
| `ai_context` | *(absent)* | — | ❌ | — | `dbt.metrics.ai_context` exists but is Proprietary/Fusion-specific; not a Discovery-public field. Defer until a UI consumer exists |
| `config` | *(absent)* | — | ❌ | — | `dbt.metrics.config` JSON exists but has no Discovery-public schema; defer until a UI consumer exists. Mirrors `TestDetail` Risk: the GraphQL `config` blob has no FE consumer for metrics either |
| `refs` | *(absent)* | — | ❌ | — | `dbt.metrics.refs` JSON exists but is denormalized into `depends_on` via `dbt.edges`; do not duplicate |
| `sources` | *(absent)* | — | ❌ | — | Same rationale as `refs` |
| `depends_on_macros` | *(absent)* | — | ❌ | — | Denormalized into the generic `depends_on` edge view if needed; metrics rarely reference macros directly. Defer until a UI consumer exists |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific; no parquet path |
| `last_run_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific run ID; no parquet path |
| `last_job_definition_id` | *(absent)* | — | ❌ | — | Class B: Cloud-specific job ID; no parquet path |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface MetricDetail {
  unique_id: string;
  name: string;
  resource_type: "metric";
  package_name: string | null;
  label: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  fqn: string[];
  tags: string[];
  metric_type: string | null;       // "simple" | "ratio" | "derived" | "cumulative" | "conversion"
  type_params: Record<string, unknown> | null;
  filter: Record<string, unknown> | null;
  time_granularity: string | null;  // "day" | "week" | "month" | "quarter" | "year"
  semantic_model_name: string | null;
  input_metric_names: string[];
  group_name: string | null;
  meta: Record<string, unknown> | null;
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
  created_at: number | null;        // ADR-5: per-resource "Definition updated as of …" timestamp; epoch seconds
}

// EdgeRef is shared with ModelDetail, SourceDetail, SeedDetail, SnapshotDetail, TestDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`type_params` JSON deserialization shape.** `dbt.metrics.parquet` stores
   `type_params` as a JSON-serialized string (`Option<String>` in `MetricRow`). The
   handler must deserialize it into a JSON object for the response; returning the raw
   string would leak an implementation detail and break the FE Zod schema
   (`zTypeParams`). Verify DuckDB `json_parse`/`json` extension availability in the
   `dbt-docs-server` query path, or perform deserialization in Rust after the Arrow
   batch is returned. Same risk applies to `filter`, `meta`, `refs`, `sources`, `config`.

2. **`filter` shape divergence between manifest and renderer — [DECIDED: our contract follows manifest/parquet truth].** The dbt-ui Zod schema in `discovery/metric.ts` declares `zFilter` as `{ where_sql_template: string | null }`, but `MetricFilterTable.tsx` reads `metric.filter.where_filters[].where_sql_template`. The GraphQL `MetricDefinitionNode.filter` is typed as untyped `JSONObject`. The manifest v10 shape (which `dbt.metrics.metric_filter` mirrors via `jjson(m, "filter")`) has `{ where_filters: [{ where_sql_template }] }`. **The dbt-docs-server contract uses the nested `where_filters[]` shape that matches the manifest.** The FE Zod inconsistency is an internal dbt-ui issue — flag it separately so the FE schema is fixed before consuming this endpoint; it doesn't block our contract.

3. **`formula` is fetched but absent from parquet — [RESOLVED].** The GraphQL hook
   selects `formula`, but `MetricRow` has no `formula` column. Empirically verified
   against `sl-schema-evolution/sample_project/target/index/dbt.metrics.parquet`: for
   `derived` metrics the expression lives at `type_params.expr` (observed value:
   `"total_enrollments / total_classes_enrolled"`). The contract treats `formula` as
   ❌ Class B; the FE reads `type_params.expr` directly when it needs the derived-metric
   expression. No dbt-index change required.

4. **`meta` JSONB presence — [RESOLVED].** Same parquet-storage shape as on
   `SourceDetail` / `SeedDetail` (`meta` stored as `Option<String>` JSON). Empirically
   verified: the `meta` column is present in `dbt.metrics.parquet` (confirmed via the
   sample project schema). Handler must JSON-parse on the way out; rolls into the
   cross-cutting JSON helper decision (see Open Question Q4 in the PR description).

5. **`depends_on` may include `semantic_model.*` and `metric.*` mixed.** Unlike model
   `depends_on` (which is typically `model.*`/`source.*`/`seed.*`), metric upstream
   edges depend on the metric `type`: `simple`/`cumulative` depend on a `semantic_model.*`;
   `ratio`/`derived` depend on other `metric.*` entries. The FE must not assume a single
   upstream resource type. Document explicitly so FE engineers don't filter edges by
   `edge_type === "model"` and silently drop semantic model parents.

6. **Q35 semantic-layer blocker — [RETIRED].** Prior framing claimed
   `dbt.metrics.parquet` would be empty for OSS Core projects pending Core v2.
   Empirically refuted: the `sl-schema-evolution/sample_project` index contains 43
   metric rows across all 5 `metric_type` variants, written by the standard
   index path (`.artifact_meta.json: write_source: "DirectWrite"`). The SL parquet
   tables are emitted today by any project with a `semantic_manifest.json`, regardless
   of toolchain. No 404 risk; no capability gate needed.

7. **`run_generated_at` mapping — [RESOLVED].** Prior framing claimed no per-metric
   timestamp existed. Empirically refuted: `dbt.metrics.parquet` has **both**
   `created_at: double` (epoch seconds, when the metric was first ingested) and
   `ingested_at: timestamp[us, tz=UTC]` (when this index write touched the row).
   Recommendation: surface `created_at` as the resource's "Definition updated as of …"
   timestamp in the response (Core 🔧). FE no longer needs a project-level fallback.

8. **No pagination cap on `depends_on`/`referenced_by`.** Same risk as `ModelDetail`
   Risk #5 and `SnapshotDetail` Risk #4. A `derived` metric that aggregates many input
   metrics, or a popular metric referenced by many saved queries, would return an
   unbounded array. Add a `?first=` cap with `truncated: true` consistent with the
   model and snapshot contracts.

## Design notes — `GET /api/v1/saved_queries/:id`

Two judgment calls in this contract. Both are now codified by ADR-5 (Semantic-Layer
resources omit `execution_info` entirely) and CC-7 (JSON-string columns are parsed
handler-side). The notes below remain as supporting context for the saved-queries
endpoint specifically:

  1. Lives in a dedicated `dbt.saved_queries` parquet table (no `dbt.nodes` row).
  2. Has no `execution_info` analogue — saved queries are a Semantic Layer definition,
     not a build target. `dbt build` does not produce a `dbt_rt.run_results` row for a
     `saved_query.*` unique_id.

**1. JSON-column unpacking convention (`query_params`, `exports`).**

The parquet schema (`SavedQueryRow` in `crates/dbt-index/src/parquet.rs:1252`) stores
`query_params` and `exports` as opaque JSON strings, not as Arrow nested types. The
contract returns them as fully parsed JSON objects whose shape matches the Discovery
API field-for-field. This is consistent with CC-2 (preserve nested Discovery shape) and
extends precedent set by `catalog.stats[]`, which is also handler-parsed. Risk #1
captures the DuckDB `json_extract` / parse-side cost. Decision: the handler parses on
the way out; the REST contract MUST NOT expose stringified JSON.

**2. No `execution_info`, no run-status capability gate.**

Saved queries are not built. They are queried at runtime through the Semantic Layer
service against `dbt-mantle`/`dbt_sl`. The on-disk `dbt_rt.run_results.parquet`
contains rows for `model.*`, `seed.*`, `snapshot.*`, `test.*`, `unit_test.*` —
**never** `saved_query.*`. Therefore this contract omits `execution_info`, does not
participate in `has_run_results`, and the Field reference table has no
`Core-conditional` rows for run state. The closest analogue — "definition last
generated at" — is exposed as `created_at` (parquet-backed; epoch seconds in
`dbt.saved_queries.created_at`) without a capability gate.

---

## `GET /api/v1/saved_queries/:id`

Powers: `SavedQueryView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SavedQueryView.tsx`
GraphQL hook: `packages/metadata/dbt-explorer/src/hooks/discovery/savedQuery.ts` (`GetSavedQueryByUniqueId`) and `src/hooks/dbtStrategy/useSavedQuery.ts`

Saved queries are Semantic Layer entities that bundle a metric selection
(`metrics[]`), grouping (`group_by[]`), filtering (`where.where_filters[]`), and
optional ordering/limit into a reusable query, plus zero-or-more **exports** — saved
materializations of the query result into a warehouse table or view. They live in
`dbt.saved_queries.parquet` (see `SavedQueryRow` in `crates/dbt-index/src/parquet.rs`),
**not** in `dbt.nodes` — they have no SQL body, no warehouse relation of their own
(exports materialize into separate relations), no run results, no columns, and no
catalog. `depends_on` typically references the metrics and semantic models the query
selects from; `referenced_by` is generally empty (nothing depends on a saved query in
the build graph). `query_params` and `exports` are parquet-stored JSON strings — the
handler parses them server-side; the REST contract returns nested JSON objects per
CC-2.

### Example response

Fields marked `// 🔧` are not yet returned — there is no `/api/v1/saved_queries/:id`
handler today. Fields marked `// 🔍` are parquet presence unverified — confirm
schema before implementing.

```json
{
  "unique_id": "saved_query.jaffle_shop.weekly_revenue_summary",
  "name": "weekly_revenue_summary",
  "resource_type": "saved_query",
  "label": "Weekly Revenue Summary",
  "package_name": "jaffle_shop",
  "description": "Weekly revenue by region, materialized to the analytics schema.",
  "original_file_path": "models/semantic/saved_queries.yml",
  "file_path": "models/semantic/saved_queries.yml",
  "fqn": ["jaffle_shop", "semantic", "weekly_revenue_summary"],
  "tags": ["finance", "weekly"],
  "group_name": "finance",
  "created_at": 1747320731.0,
  "query_params": {
    "metrics": ["revenue", "order_count"],
    "group_by": ["customer__region", "metric_time__week"],
    "order_by": ["-metric_time__week"],
    "limit": 1000,
    "where": {
      "where_filters": [
        { "where_sql_template": "{{ Dimension('customer__region') }} != 'INTERNAL'" }
      ]
    }
  },
  "exports": [
    {
      "name": "weekly_revenue_summary__warehouse",
      "config": {
        "alias": "weekly_revenue_summary",
        "export_as": "table",
        "schema": "analytics",
        "database": "prod"
      }
    }
  ],
  "depends_on": [
    { "unique_id": "metric.jaffle_shop.revenue", "edge_type": "metric" },
    { "unique_id": "metric.jaffle_shop.order_count", "edge_type": "metric" },
    { "unique_id": "semantic_model.jaffle_shop.customers", "edge_type": "semantic_model" }
  ],
  "referenced_by": []
}
```

There are no capability-gated fields on this response: saved queries have no
`execution_info`, no `catalog`, no `freshness`. See Design note 2.

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"saved_query.pkg.name"`; from `dbt.saved_queries.unique_id` |
| `name` | `string` | Core | 🔧 | — | from `dbt.saved_queries.name` |
| `resource_type` | `"saved_query"` | Core | 🔧 | — | Always `"saved_query"` for this endpoint |
| `label` | `string \| null` | Core | 🔧 | — | Display label; from `dbt.saved_queries.label` |
| `package_name` | `string \| null` | Core | 🔧 | — | from `dbt.saved_queries.package_name` |
| `description` | `string \| null` | Core | 🔧 | — | from `dbt.saved_queries.description` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | YAML file path relative to project root |
| `file_path` | `string \| null` | Core | 🔧 | — | from `dbt.saved_queries.file_path`; same `.yml` as `original_file_path` for most projects |
| `fqn` | `string[]` | Core | 🔧 | — | from `dbt.saved_queries.fqn` |
| `tags` | `string[]` | Core | 🔧 | — | from `dbt.saved_queries.tags` |
| `group_name` | `string \| null` | Core | 🔧 | — | from `dbt.saved_queries.group_name` |
| `created_at` | `number \| null` | Core | 🔧 | — | Epoch seconds (float); from `dbt.saved_queries.created_at`. Per ADR-5, this is the "Definition updated as of …" timestamp surfaced to `SavedQueryView`; Discovery API analogue is `runGeneratedAt` — see Risk #5. Empirically verified column present in the sample project. |
| `query_params` | `QueryParams \| null` | Core | 🔧 | — | Parsed from JSON-string column `dbt.saved_queries.query_params`; see Design note 1 |
| `query_params.metrics` | `string[]` | Core | 🔧 | — | List of metric names selected by the query |
| `query_params.group_by` | `string[]` | Core | 🔧 | — | List of dimension or entity references (e.g., `"customer__region"`) |
| `query_params.order_by` | `string[]` | Core | 🔍 | — | Discovery API returns flat strings (e.g., `"-metric_time__week"`); confirm parquet JSON shape — see Risk #2 |
| `query_params.limit` | `number \| null` | Core | 🔧 | — | Row cap applied at SL query time |
| `query_params.where` | `QueryParamsWhere \| null` | Core | 🔧 | — | Wrapper object holding `where_filters[]` |
| `query_params.where.where_filters` | `WhereFilter[]` | Core | 🔧 | — | Empty array if no filters |
| `query_params.where.where_filters[*].where_sql_template` | `string` | Core | 🔧 | — | Jinja-templated SQL filter — Discovery API: `whereSqlTemplate` (CC-1 rewrites to snake_case) |
| `exports` | `Export[]` | Core | 🔧 | — | Parsed from JSON-string column `dbt.saved_queries.exports`; empty array if no exports defined |
| `exports[*].name` | `string` | Core | 🔧 | — | Export identifier (used as the materialized relation suffix) |
| `exports[*].config` | `ExportConfig \| null` | Core | 🔧 | — | Materialization config; `null` only if YAML omits the `config:` block |
| `exports[*].config.alias` | `string \| null` | Core | 🔧 | — | Override for the materialized relation name |
| `exports[*].config.export_as` | `string \| null` | Core | 🔧 | — | `"table"` · `"view"` — Discovery API: `exportAs` (CC-1 rewrites to snake_case) |
| `exports[*].config.schema` | `string \| null` | Core | 🔧 | — | Schema for the materialized relation |
| `exports[*].config.database` | `string \| null` | Core | 🔧 | — | Database for the materialized relation |
| `depends_on` | `EdgeRef[]` | Core | 🔧 | — | 1-hop upstream from `dbt.edges`; typically metrics + semantic models |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | |
| `depends_on[*].edge_type` | `string` | Core | 🔧 | — | |
| `referenced_by` | `EdgeRef[]` | Core | 🔧 | — | 1-hop downstream from `dbt.edges`; typically empty |
| `referenced_by[*].unique_id` | `string` | Core | 🔧 | — | |
| `referenced_by[*].edge_type` | `string` | Core | 🔧 | — | |
| `parents` | *(absent)* | — | ❌ | — | Discovery API exposes `parents` (full node summaries). dbt-docs-server uses `depends_on` (edge refs only). FE caller resolves names via `GET /api/v1/nodes/:id` if needed. |
| `children` | *(absent)* | — | ❌ | — | Same as `parents` — covered by `referenced_by`. |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud project ID; not in local parquet |
| `run_generated_at` | *(absent)* | — | ❌ | — | Class B: Cloud manifest snapshot timestamp. Closest local analogue is `created_at` — see Risk #5 |
| `execution_info` | *(absent)* | — | ❌ | — | Saved queries are never executed by `dbt build`; no `dbt_rt.run_results` row exists. See Design note 2. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `columns` | *(absent)* | — | ❌ | — | Saved queries have no declared columns — the column set is derived at SL query time from `query_params.metrics` and `query_params.group_by` |
| `catalog` | *(absent)* | — | ❌ | — | Saved queries have no warehouse relation of their own (exports do, but those are separate models from dbt's perspective) |
| `materialized` | *(absent)* | — | ❌ | — | Materialization lives on each export (`exports[*].config.export_as`), not on the saved query itself |
| `relation_name` | *(absent)* | — | ❌ | — | See `materialized` — relations are per-export |
| `raw_code` | *(absent)* | — | ❌ | — | Saved queries are declarative YAML, not SQL |
| `compiled_code` | *(absent)* | — | ❌ | — | Saved queries are declarative YAML, not SQL |
| `meta` | *(absent)* | — | ❌ | — | `dbt.saved_queries` schema has no `meta` column (unlike `dbt.nodes`); the `config` column exists but is not exposed |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api |
| `patch_path` | *(absent)* | — | ❌ | — | Class B: YAML-only resource — `original_file_path` IS the `.yml` file containing the saved query definition; the patch concept does not apply (a "patch" is a separate YAML that augments a non-YAML primary definition, e.g. `.sql` + `schema.yml`). Discovery's `patchPath` would be null or duplicate `originalFilePath` for this resource. |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SavedQueryDetail {
  unique_id: string;
  name: string;
  resource_type: "saved_query";
  label: string | null;
  package_name: string | null;
  description: string | null;
  original_file_path: string | null;
  file_path: string | null;
  fqn: string[];
  tags: string[];
  group_name: string | null;
  created_at: number | null;
  query_params: QueryParams | null;
  exports: Export[];
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
}

interface QueryParams {
  metrics: string[];
  group_by: string[];
  order_by: string[];
  limit: number | null;
  where: QueryParamsWhere | null;
}

interface QueryParamsWhere {
  where_filters: WhereFilter[];
}

interface WhereFilter {
  where_sql_template: string;
}

interface Export {
  name: string;
  config: ExportConfig | null;
}

interface ExportConfig {
  alias: string | null;
  export_as: string | null;
  schema: string | null;
  database: string | null;
}

// EdgeRef is shared with ModelDetail, SourceDetail, SeedDetail, SnapshotDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **`query_params` and `exports` are JSON strings in parquet.** Both columns are
   `Option<String>` in `SavedQueryRow` (`crates/dbt-index/src/parquet.rs:1261-1262`).
   The handler must parse them server-side and emit nested JSON objects matching the
   contract above. Options: (a) DuckDB `json_extract` per field, or (b) read the raw
   string and parse in Rust via `serde_json`. Option (b) is simpler and avoids one
   query plan per nested field; recommend it unless profiling shows a hot path. If
   parsing fails for a malformed row, return `null` for the affected field — never a
   stringified blob.

2. **`query_params.order_by` shape is unverified.** The Discovery GraphQL surface
   returns `orderBy` as a flat `string[]` (each entry encoding direction with a `-`
   prefix). The parquet JSON blob is whatever the dbt parser emits — it could be flat
   strings, or it could be objects like `{ "metric": "...", "descending": true }`.
   Inspect a real `dbt.saved_queries.parquet` `query_params` value before implementing.
   If the shape diverges from Discovery, adopt the parquet shape and document the
   transformation here. Recommend treating this field as 🔍 until verified.

3. **`depends_on` edges may include macros, not just metrics/semantic models.**
   `SavedQueryRow` has both `depends_on_nodes: Vec<String>` and
   `depends_on_macros: Vec<String>`. The Discovery API's `parents` field surfaces
   resource nodes (metrics, semantic models), not macros. The handler should
   probably restrict `depends_on` to non-macro edges — otherwise the FE will render
   a `Macro` chip for every Jinja templating dependency, which is noise. Decide:
   include macros in `depends_on` (consistent with `ModelDetail`), or filter them
   out (consistent with the UI expectation). Default to the model precedent
   (include); flag in implementation review if the UX feels wrong.

4. **`referenced_by` is typically empty but not guaranteed.** A saved query can
   theoretically be referenced by an `exposure`. Verify `dbt.edges.parquet` records
   `parent_unique_id` for `saved_query.*` when an exposure depends on a saved query.
   If yes, this contract is correct as-is. If no (exposures only reference models),
   strike `referenced_by` from the response. Verify with a project that exercises
   the case before implementing.

5. **`run_generated_at` ≠ `created_at`.** The Discovery API field `runGeneratedAt`
   is the manifest-generation timestamp from CodexDB, which is a project-wide
   concept (when the latest manifest was ingested). The parquet `created_at` is a
   per-row epoch-seconds float that may represent the parse-time of the saved query
   YAML. These are **not** the same thing. The SavedQueryView header renders
   "Definition updated as of <date>" using `runGeneratedAt` — if `created_at` is
   project-wide-constant in parquet, it serves the same UX purpose; if it varies
   per-row, it conveys something more granular and useful. Either way, document the
   semantic difference in the FE-facing API docs so engineers don't expect Cloud
   parity.

6. **No execution_info means no `has_run_results` gate on this endpoint.** This is
   intentional (Design note 2), but worth restating for any future engineer who
   sees every other detail endpoint participate in `has_run_results` and asks why
   saved queries don't. Document at the top of the handler: "Saved queries are
   declarative SL definitions; they have no run-time execution status. If a saved
   query's exports are materialized, run status lives on the resulting model
   nodes, queryable via `GET /api/v1/models/:id`."

7. **`exports[*].config` may be `null` for sparse YAML.** A saved query with no
   `config:` block under its export still has a `name`. The handler must tolerate
   `{ "name": "...", "config": null }` rather than synthesizing an empty
   `ExportConfig`. The FE will render `config: null` as "No materialization
   configured" rather than four empty cells.

## Design notes — `GET /api/v1/semantic_models/:id`

Three non-obvious decisions arise here. The coordinator should decide whether any of these warrant promotion to a full ADR before this contract is merged into `API-CONTRACTS.md`.

**1. `entities`, `dimensions`, `measures` are inlined as arrays, not promoted to sub-resources.**
The dbt-ui detail page renders all three on the same view via tabs and section components (`DimensionsView`, `MeasuresView`, `SemanticModelEntities`) — they are conceptually part of the semantic model itself, not independent resources. Inline mirrors the GraphQL shape (Discovery returns them on the `SemanticModelDefinitionNode`) and is consistent with how `columns[]` is inlined on `ModelDetail`. The fan-out is bounded by spec authorship (typically tens of entries, not hundreds) so no pagination cap is proposed. If a future "metric usage" surface needs to look up measures across all semantic models, a `GET /api/v1/measures` collection endpoint can be added additively.

**2. No `execution_info`, no `catalog`, no capability gating.**
Semantic models are **spec-only** — they declare entities/dimensions/measures on top of an existing model but are not themselves executed against the warehouse. Their parquet source (`dbt.semantic_models` + `dbt.semantic_{entities,measures,dimensions}`) is written by `dbt parse` / `dbt build` during semantic-manifest ingestion (see `crates/dbt-index/src/ingest/semantic_manifest.rs`) and contains no run-result columns. `RESOURCES_WITH_EXECUTION_INFO` in dbt-ui (`hooks/discovery/types.ts:53`) confirms only `Model | Seed | Snapshot` carry execution_info. ADR-2's `has_run_results` flag does not apply. ADR-4 (bare execution_info naming) is moot.

**3. Measure `agg` and dimension `type` are surfaced as raw strings, not discriminated unions.**
The dbt-ui `SemanticAspectCard` renders `agg` and `type` as uppercase badges (`SUM`, `COUNT_DISTINCT`, `CATEGORICAL`, `TIME`) with no behavior conditional on the value. MetricFlow defines a closed set of enum values for both, but the consumer treats them as opaque strings. Keep as `string | null` rather than introducing a TypeScript union — keeps the contract stable as MetricFlow extends the enums and matches the precedent set by `materialized` and `access_level` on `ModelDetail`. A measure's `agg_params` (e.g., the `percentile` argument for `percentile` agg) is exposed alongside as an opaque JSON-string column (see `dbt.semantic_measures.agg_params`); if a frontend needs typed access it should parse it locally.

---

## `GET /api/v1/semantic_models/:id`

Powers: `SemanticModelView` / `ResourceDetailsPage` in dbt-ui.
dbt-ui component: `packages/metadata/dbt-explorer/src/pages/account/project/resource/details/components/DetailPages/SemanticModelView.tsx`
GraphQL hooks: `packages/metadata/dbt-explorer/src/hooks/dbtStrategy/useSemanticModel.ts` → `src/hooks/discovery/semanticModel.ts` (`GetSemanticModelByUniqueId`)

Semantic models are MetricFlow / Semantic Layer specs that bind structured aggregation surfaces (entities, dimensions, measures) onto an underlying dbt model. They are **spec-only** — defined in `.yml` and parsed during `dbt parse` / `dbt build`, but never themselves executed against the warehouse. Their parquet source is the `dbt.semantic_models` table (one row per semantic model) plus three sibling tables joined on the parent `unique_id`: `dbt.semantic_entities`, `dbt.semantic_measures`, `dbt.semantic_dimensions`. Because there are no run results, this endpoint has **no `execution_info`, no `catalog`, and no capability gating** — every Class A field is unconditional. The upstream relation is the model the semantic model is built on (the `model:` reference in YAML, captured as `dbt.semantic_models.model` and as a single edge in `dbt.edges`). Downstream consumers are metrics that reference the measures and saved queries that select the dimensions.

### Example response

Fields marked `// 🔧` are not yet returned — there is no handler today; expect almost every field to be a fresh `SELECT`.
Fields marked `// 🔍` are parquet presence unverified — confirm before implementing.

```json
{
  "unique_id": "semantic_model.jaffle_shop.orders",
  "name": "orders",
  "resource_type": "semantic_model",
  "package_name": "jaffle_shop",
  "description": "Semantic model over the orders fact table.",
  "label": "Orders",
  "original_file_path": "models/semantic_models.yml",
  "file_path": "semantic_models.yml",
  "tags": ["finance", "semantic"],
  "fqn": ["jaffle_shop", "semantic_models", "orders"],
  "meta": { "owner": "data-eng" },
  "group_name": "finance",
  "model": {
    "unique_id": "model.jaffle_shop.fct_orders",
    "name": "fct_orders",
    "access_level": "public",
    "alias": "fct_orders"
  },
  "primary_entity": "order",
  "entities": [
    {
      "name": "order",
      "type": "primary",
      "description": "Unique order identifier.",
      "label": null,
      "expr": "order_id",
      "role": null
    },
    {
      "name": "customer",
      "type": "foreign",
      "description": "Customer that placed the order.",
      "label": null,
      "expr": "customer_id",
      "role": null
    }
  ],
  "dimensions": [
    {
      "name": "ordered_at",
      "type": "time",
      "description": "Timestamp the order was placed.",
      "label": null,
      "expr": "ordered_at",
      "is_partition": false,
      "time_granularity": "day",
      "type_params": { "time_granularity": "day" }
    },
    {
      "name": "status",
      "type": "categorical",
      "description": "Order lifecycle status.",
      "label": null,
      "expr": "status",
      "is_partition": false,
      "time_granularity": null,
      "type_params": null
    }
  ],
  "measures": [
    {
      "name": "order_total",
      "agg": "sum",
      "description": "Sum of order totals.",
      "label": null,
      "expr": "amount",
      "create_metric": true,
      "agg_time_dimension": "ordered_at",
      "agg_params": null,
      "non_additive_dimension": null
    },
    {
      "name": "order_count",
      "agg": "count",
      "description": "Number of orders.",
      "label": null,
      "expr": "1",
      "create_metric": false,
      "agg_time_dimension": "ordered_at",
      "agg_params": null,
      "non_additive_dimension": null
    }
  ],
  "depends_on": [
    { "unique_id": "model.jaffle_shop.fct_orders", "edge_type": "ref" }
  ],
  "referenced_by": [
    { "unique_id": "metric.jaffle_shop.total_orders", "edge_type": "metric" },
    { "unique_id": "saved_query.jaffle_shop.orders_by_month", "edge_type": "saved_query" }
  ],
  "created_at": 1747432300.5
}
```

`created_at` is the per-resource "Definition updated as of …" timestamp per ADR-5
(epoch seconds, sourced from `dbt.semantic_models.created_at`).

### Field reference

Status legend: ✅ returned today · 🔧 needs backend change · 🔍 verify parquet schema · ❌ excluded (no parquet path)

There is no `GET /api/v1/semantic_models/:id` handler today, so every Class A row below is 🔧 (or 🔍 where parquet presence is unverified). Class A rows are the bulk of the contract.

| Field | Type | Tier | Status | Capability gate | Notes |
|---|---|---|---|---|---|
| `unique_id` | `string` | Core | 🔧 | — | e.g., `"semantic_model.pkg.name"` |
| `name` | `string` | Core | 🔧 | — | From `dbt.semantic_models.name` |
| `resource_type` | `"semantic_model"` | Core | 🔧 | — | Always `"semantic_model"` for this endpoint |
| `package_name` | `string \| null` | Core | 🔧 | — | From `dbt.semantic_models.package_name` |
| `description` | `string \| null` | Core | 🔧 | — | |
| `label` | `string \| null` | Core | 🔧 | — | Human-readable label from `dbt.semantic_models.label` |
| `original_file_path` | `string \| null` | Core | 🔧 | — | YAML file containing the semantic model spec |
| `file_path` | `string \| null` | Core | 🔧 | — | `dbt.semantic_models.file_path` (relative) |
| `patch_path` | *(absent)* | — | ❌ | — | Class B: YAML-only resource — `dbt.semantic_models` parquet has no `patch_path` column. `original_file_path` IS the YAML file containing the spec; the patch concept does not apply (a "patch" is a separate YAML that augments a non-YAML primary definition, e.g. `.sql` + `schema.yml`). Discovery's `patchPath` would be null or duplicate `originalFilePath` for this resource. |
| `tags` | `string[]` | Core | 🔧 | — | Empirically verified absent at the top-level of `dbt.semantic_models.parquet`. Handler must extract from the `config` JSON column via `json_extract(config, '$.tags')`, defaulting to `[]` when absent — see Risk #2 |
| `fqn` | `string[]` | Core | 🔧 | — | `dbt.semantic_models.fqn` (3-part: `[pkg, semantic_models, name]`) |
| `meta` | `Record<string, unknown> \| null` | Core | 🔍 | — | Likely embedded in `config` JSON column on `dbt.semantic_models`; same risk class as model `meta` |
| `group_name` | `string \| null` | Core | 🔧 | — | `dbt.semantic_models.group_name` |
| `model` | `UpstreamModelRef \| null` | Core | 🔧 | — | The model the semantic model is built on; from `dbt.semantic_models.model` joined to `dbt.nodes` for `access_level`/`alias` |
| `model.unique_id` | `string` | Core | 🔧 | — | Direct read of `dbt.semantic_models.model` — empirically verified to store an already-resolved `model.{pkg}.{name}` unique_id (e.g. `"model.another_semantic_model"`), not a raw `ref()` string. No edges JOIN needed. |
| `model.name` | `string` | Core | 🔧 | — | |
| `model.access_level` | `string \| null` | Core | 🔧 | — | Pulled from the joined model row |
| `model.alias` | `string \| null` | Core | 🔧 | — | Pulled from the joined model row |
| `primary_entity` | `string \| null` | Core | 🔧 | — | `dbt.semantic_models.primary_entity`; entity name designated primary at the model level |
| `entities` | `SemanticEntity[]` | Core | 🔧 | — | All rows of `dbt.semantic_entities` where `unique_id` matches; empty array if none |
| `entities[*].name` | `string` | Core | 🔧 | — | |
| `entities[*].type` | `string \| null` | Core | 🔧 | — | `"primary"` · `"natural"` · `"foreign"` · `"unique"` (MetricFlow enum) |
| `entities[*].description` | `string \| null` | Core | 🔧 | — | |
| `entities[*].label` | `string \| null` | Core | 🔧 | — | |
| `entities[*].expr` | `string \| null` | Core | 🔧 | — | SQL expression resolving to the entity column |
| `entities[*].role` | `string \| null` | Core | 🔧 | — | `dbt.semantic_entities.entity_role`; aliased to `role` in JSON to drop redundant prefix |
| `dimensions` | `SemanticDimension[]` | Core | 🔧 | — | All rows of `dbt.semantic_dimensions` where `unique_id` matches |
| `dimensions[*].name` | `string` | Core | 🔧 | — | |
| `dimensions[*].type` | `string \| null` | Core | 🔧 | — | `"time"` · `"categorical"` (MetricFlow enum) — from `dbt.semantic_dimensions.dimension_type`; aliased to `type` in JSON to match GraphQL |
| `dimensions[*].description` | `string \| null` | Core | 🔧 | — | |
| `dimensions[*].label` | `string \| null` | Core | 🔧 | — | |
| `dimensions[*].expr` | `string \| null` | Core | 🔧 | — | SQL expression resolving to the dimension column |
| `dimensions[*].is_partition` | `boolean \| null` | Core | 🔧 | — | Whether dimension is a partition column (time dimensions only) |
| `dimensions[*].time_granularity` | `string \| null` | Core | 🔧 | — | `"day"` · `"week"` · `"month"` etc.; populated only for `type == "time"` |
| `dimensions[*].type_params` | *(absent)* | — | ❌ | — | Class B: empirically verified — `dbt.semantic_dimensions` parquet schema has no `type_params` column. Equivalent information is split across `dimension_type` (categorical/time), `time_granularity`, and `validity_params` (JSON-encoded), each of which IS a parquet column and is exposed directly. The FE should consume those instead of expecting a combined `type_params` object — see Risk #4 |
| `measures` | `SemanticMeasure[]` | Core | 🔧 | — | All rows of `dbt.semantic_measures` where `unique_id` matches |
| `measures[*].name` | `string` | Core | 🔧 | — | |
| `measures[*].agg` | `string \| null` | Core | 🔧 | — | `"sum"` · `"count"` · `"count_distinct"` · `"average"` · `"max"` · `"min"` · `"percentile"` · `"sum_boolean"` · `"median"` (MetricFlow enum) |
| `measures[*].description` | `string \| null` | Core | 🔧 | — | |
| `measures[*].label` | `string \| null` | Core | 🔧 | — | |
| `measures[*].expr` | `string \| null` | Core | 🔧 | — | SQL expression resolving to the measure column |
| `measures[*].create_metric` | `boolean \| null` | Core | 🔧 | — | Whether this measure auto-generates a simple metric |
| `measures[*].agg_time_dimension` | `string \| null` | Core | 🔧 | — | Time dimension this measure is aggregated over (for time-aware metrics) |
| `measures[*].agg_params` | `Record<string, unknown> \| null` | Core | 🔧 | — | Opaque JSON; e.g., `{"percentile": 0.95, "use_discrete_percentile": false}` for `agg == "percentile"`. Stored as JSON-string column; parse before emission. |
| `measures[*].non_additive_dimension` | `Record<string, unknown> \| null` | Core | 🔧 | — | Opaque JSON; defines a dimension along which the measure cannot be naively summed (e.g., end-of-period balances) |
| `depends_on` | `EdgeRef[]` | Core | 🔧 | — | 1-hop upstream — exactly one entry pointing at the underlying model. From `dbt.edges` filtered by `from = this.unique_id`. |
| `depends_on[*].unique_id` | `string` | Core | 🔧 | — | |
| `depends_on[*].edge_type` | `string` | Core | 🔧 | — | Typically `"ref"` |
| `referenced_by` | `EdgeRef[]` | Core | 🔧 | — | 1-hop downstream — metrics and saved queries that consume this semantic model. From `dbt.edges` filtered by `to = this.unique_id`. |
| `referenced_by[*].unique_id` | `string` | Core | 🔧 | — | |
| `referenced_by[*].edge_type` | `string` | Core | 🔧 | — | |
| `materialized` | *(absent)* | — | ❌ | — | Semantic models are spec-only; no materialization |
| `database_name` | *(absent)* | — | ❌ | — | Semantic models reference a model; no warehouse relation of their own |
| `schema_name` | *(absent)* | — | ❌ | — | Same reason |
| `relation_name` | *(absent)* | — | ❌ | — | Same reason |
| `identifier` | *(absent)* | — | ❌ | — | Same reason |
| `access_level` | *(absent)* | — | ❌ | — | Not applicable; semantic models inherit governance from the underlying model |
| `contract_enforced` | *(absent)* | — | ❌ | — | Not applicable |
| `raw_code` | *(absent)* | — | ❌ | — | Semantic models have no SQL body (YAML spec only) |
| `compiled_code` | *(absent)* | — | ❌ | — | Same reason |
| `columns` | *(absent)* | — | ❌ | — | Columns are surfaced as dimensions/measures/entities on the parent model; not redundantly on the semantic model |
| `created_at` | `number \| null` | Core | 🔧 | — | Epoch seconds (float); from `dbt.semantic_models.created_at`. Per ADR-5, this is the "Definition updated as of …" timestamp surfaced to `SemanticModelView` (replaces the prior recommendation to fall back to project-level `runGeneratedAt`). Empirically verified column present across 10 rows in the sample project. |
| `execution_info` | *(absent)* | — | ❌ | — | Class B: semantic models are not executed (no `dbt_rt.run_results` rows); `RESOURCES_WITH_EXECUTION_INFO` in dbt-ui excludes them — see Design note #2. Per ADR-5 the field is omitted from `DefinitionNodeBase` entirely — this row is documentation only. |
| `catalog` | *(absent)* | — | ❌ | — | Class B: no warehouse relation; no `dbt.catalog_tables` row |
| `freshness` | *(absent)* | — | ❌ | — | Source-only concept |
| `health_issues` | *(absent)* | — | ❌ | — | Class B: no parquet path; `subGraphs: ['internal']` in codex-api — matches `ModelDetail` Risk #7 |
| `project_id` | *(absent)* | — | ❌ | — | Class B: Cloud-tier concept |
| `run_generated_at` | *(absent)* | — | ❌ | — | Cloud-tier run timestamp; dbt-ui header uses it for "Definition updated as of …" — replace with `dbt.project.last_indexed_at` at the project level if needed (Class B at the resource level) — see Risk #6 |
| `job_definition_id` | *(absent)* | — | ❌ | — | Class B: Cloud scheduler concept |
| `run_id` | *(absent)* | — | ❌ | — | Class B: Cloud run ID |
| `account_id` | *(absent)* | — | ❌ | — | Class B: Cloud tenant concept |
| `environment_id` | *(absent)* | — | ❌ | — | Class B: Cloud environment concept |
| `dbt_version` | *(absent)* | — | ❌ | — | Class B at the resource level; available on `GET /api/v1/project` if needed |

### Type definition

For codegen reference. The field reference table above is the authoritative contract.

```typescript
interface SemanticModelDetail {
  unique_id: string;
  name: string;
  resource_type: "semantic_model";
  package_name: string | null;
  description: string | null;
  label: string | null;
  original_file_path: string | null;
  file_path: string | null;
  tags: string[];
  fqn: string[];
  meta: Record<string, unknown> | null;
  group_name: string | null;
  model: UpstreamModelRef | null;
  primary_entity: string | null;
  entities: SemanticEntity[];
  dimensions: SemanticDimension[];
  measures: SemanticMeasure[];
  depends_on: EdgeRef[];
  referenced_by: EdgeRef[];
  created_at: number | null;   // ADR-5: per-resource "Definition updated as of …" timestamp; epoch seconds
}

interface UpstreamModelRef {
  unique_id: string;
  name: string;
  access_level: string | null;
  alias: string | null;
}

interface SemanticEntity {
  name: string;
  type: string | null;
  description: string | null;
  label: string | null;
  expr: string | null;
  role: string | null;
}

interface SemanticDimension {
  name: string;
  type: string | null;
  description: string | null;
  label: string | null;
  expr: string | null;
  is_partition: boolean | null;
  time_granularity: string | null;
  type_params: Record<string, unknown> | null;
}

interface SemanticMeasure {
  name: string;
  agg: string | null;
  description: string | null;
  label: string | null;
  expr: string | null;
  create_metric: boolean | null;
  agg_time_dimension: string | null;
  agg_params: Record<string, unknown> | null;
  non_additive_dimension: Record<string, unknown> | null;
}

// EdgeRef is shared with ModelDetail / SourceDetail / SeedDetail
interface EdgeRef {
  unique_id: string;
  edge_type: string;
}
```

### Risk register

1. **No existing handler — greenfield endpoint with parallel parquet reads.** Unlike model/seed/snapshot (which extend the shared `dbt.nodes` SELECT), this handler reads from `dbt.semantic_models` (the row) plus three sibling tables (`dbt.semantic_entities`, `dbt.semantic_measures`, `dbt.semantic_dimensions`) filtered by the same `unique_id`. Plus a JOIN to `dbt.nodes` for the `model.access_level`/`model.alias` denormalization (the `dbt.semantic_models.model` column already stores a resolved `model.{pkg}.{name}` unique_id — see Risk #3 — so no `dbt.edges` traversal is needed for the model field). `depends_on`/`referenced_by` still come from `dbt.edges`. Implement with the existing fan-out-by-tokio-join pattern; profile against moderately-sized projects (50+ semantic models with 10–30 measures each).

2. **`tags` is not a top-level column on `dbt.semantic_models` — [RESOLVED].** Empirically confirmed against the sample project's `dbt.semantic_models.parquet`: schema is `unique_id, name, model, label, description, package_name, file_path, original_file_path, fqn, node_relation, primary_entity, defaults, depends_on_nodes, depends_on_macros, refs, group_name, config, created_at, ingested_at` — no `tags` column. The contract surfaces `tags` by extracting from the `config` JSON blob (`json_extract(config, '$.tags')`), defaulting to `[]` on absence. No dbt-index schema change required.

3. **`model.unique_id` resolution — [RESOLVED; prior framing was wrong].** Earlier draft claimed `dbt.semantic_models.model` stores a raw `ref('fct_orders')` string requiring resolution via `dbt.edges`. Empirically refuted: the column already contains a resolved `model.{pkg}.{name}` unique_id (observed: `"model.another_semantic_model"`). The handler reads `dbt.semantic_models.model` directly and JOINs `dbt.nodes` only to extract `access_level` and `alias` for the header denormalization — no `dbt.edges` traversal needed. One fewer query than originally scoped.

4. **`dimensions[*].type_params` shape — [RESOLVED].** GraphQL exposes `SemanticModelDimension.typeParams` as `JSONObject`. Empirically confirmed against `dbt.semantic_dimensions.parquet`: schema is `unique_id, name, dimension_type, description, label, expr, is_partition, time_granularity, validity_params, config, ingested_at` — no `type_params` column. The information GraphQL packs into `typeParams` is split across three first-class parquet columns (`dimension_type`, `time_granularity`, `validity_params`). Contract decision: expose those three directly and mark `type_params` ❌ Class B; the FE consumes the split fields. No dbt-index ingest change required.

5. **`measures[*].agg_params` and `non_additive_dimension` are stored as JSON-string columns.** `SemanticMeasureRow` (`crates/dbt-index/src/parquet.rs:1190`) declares both as `Option<String>` (JSON-encoded). The handler must `serde_json::from_str` each to emit them as JSON objects in the response, not as escaped strings. If parsing fails, emit `null` and log; do not bubble the error to the client.

6. **`run_generated_at` mapping — [RESOLVED].** Prior framing claimed no per-resource timestamp existed. Empirically refuted: `dbt.semantic_models.parquet` has both `created_at: double` and `ingested_at: timestamp[us, tz=UTC]`. Recommendation: surface `created_at` (epoch seconds) as the resource's "Definition updated as of …" timestamp. FE no longer needs to fall back to project-level metadata for this resource.

7. **`primary_entity` may duplicate one of the `entities[]` rows.** `dbt.semantic_models.primary_entity` stores the name of an entity (often also listed in `dbt.semantic_entities` with `entity_type = "primary"`). The dbt-ui does not currently consume `primary_entity` directly — it filters `entities[].type == "primary"`. **Decide before implementing** whether to (a) emit `primary_entity` as a denormalized convenience field alongside `entities`, (b) drop it and let the FE filter `entities`, or (c) emit only when no `entities[].type == "primary"` row exists (defensive fallback for legacy projects where the YAML only specifies `primary_entity` shorthand). Option (a) matches the parquet shape with no compute cost and is the proposed default in the example response.

8. **`depends_on` is single-entry by spec but the handler should still emit it as an array.** A semantic model declares exactly one underlying `model:`. Despite this, returning `depends_on` as an `EdgeRef[]` (length-1 array) keeps the contract uniform with `ModelDetail` and avoids special-case TypeScript narrowing. The contract is **not** to inline the upstream model into a singleton `depends_on` object — keep it array-shaped. The `model` field on the response is the convenience denormalization that adds `access_level` and `alias` for the header display.

9. **`semantic_relationships` is intentionally omitted from v0.** `dbt.semantic_relationships` (parquet schema at `crates/dbt-index/src/parquet.rs:1237`) captures `from_unique_id → to_unique_id` PK/FK relationships across semantic models. The dbt-ui detail page does not render these (no GraphQL field is fetched), so they are excluded for v0. If a future "Relationships" tab is added in `SemanticModelView`, expose as a new sub-resource `GET /api/v1/semantic_models/:id/relationships` rather than retrofitting an inline array — fan-out is unbounded across the whole semantic graph.

