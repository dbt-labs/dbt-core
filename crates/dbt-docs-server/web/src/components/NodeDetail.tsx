import { useMemo, useState } from "react";
import {
  Accordion,
  AccordionVariant,
  Badge,
  Icon,
  RyeconDuotoneStar,
  RyeconStar,
  SegmentedButton,
} from "@dbt-labs/sourdough";
import type { NodeDetail as NodeDetailT } from "../api";
import { ColumnLineageView } from "./ColumnLineageView";
import { LineageView } from "./LineageView";
import { UpgradeHookPanel } from "./UpgradeHook";
import { RESOURCE_TYPE_SINGULAR, ryeconForType } from "../lib/resourceType";

type ColumnSort = "name" | "tests";

interface Props {
  node: NodeDetailT;
  isFavorite: boolean;
  onToggleFavorite(id: string): void;
  onSelect(uniqueId: string): void;
}

export function NodeDetail({ node, isFavorite, onToggleFavorite, onSelect }: Props) {
  const [columnSort, setColumnSort] = useState<ColumnSort>("name");

  const sortedColumns = useMemo(() => {
    const arr = [...node.columns];
    if (columnSort === "tests") {
      // Per-column tests aren't in the API yet; this is a no-op for now but
      // keeps the toggle wired so the visual matches DocsView's pattern.
      arr.sort((a, b) => a.name.localeCompare(b.name));
    } else {
      arr.sort((a, b) => a.name.localeCompare(b.name));
    }
    return arr;
  }, [node.columns, columnSort]);

  const ryecon = ryeconForType(node.resource_type);
  const singular = RESOURCE_TYPE_SINGULAR[node.resource_type] ?? node.resource_type;

  return (
    <article className="node-detail">
      {/* Breadcrumbs */}
      <nav className="node-detail__breadcrumb" aria-label="Breadcrumb">
        <span>{node.package_name ?? "—"}</span>
        <span aria-hidden>/</span>
        <span>{RESOURCE_TYPE_SINGULAR[node.resource_type] ?? node.resource_type}s</span>
        <span aria-hidden>/</span>
        <span className="node-detail__breadcrumb-current">{node.name}</span>
      </nav>

      {/* Page header */}
      <header className="node-detail__header">
        <div className="node-detail__heading">
          <Icon ryecon={ryecon} size="md" alt={singular} />
          <h1 className="node-detail__title" title={node.unique_id}>
            {node.name}
          </h1>
        </div>
        <div className="node-detail__actions">
          <button
            type="button"
            className={`node-detail__fav ${isFavorite ? "is-on" : ""}`}
            onClick={() => onToggleFavorite(node.unique_id)}
            aria-pressed={isFavorite}
            aria-label={isFavorite ? "Unfavorite" : "Favorite"}
            title={isFavorite ? "Unfavorite" : "Favorite"}
          >
            <Icon
              ryecon={isFavorite ? RyeconDuotoneStar : RyeconStar}
              size="sm"
              alt=""
            />
          </button>
        </div>
      </header>

      {/* Page meta */}
      <div className="node-detail__meta">
        <code className="node-detail__uid">{node.unique_id}</code>
        {node.materialized && (
          <span><span className="muted-label">Materialization</span> {node.materialized}</span>
        )}
        {node.access_level && (
          <span><span className="muted-label">Access</span> {node.access_level}</span>
        )}
        {node.group_name && (
          <span><span className="muted-label">Group</span> {node.group_name}</span>
        )}
      </div>

      {/* Status row — Health-signals stubs (Defer per PM table). Query
          popularity removed per the Remove action in the same table. */}
      <section className="node-detail__status-row" aria-label="Status">
        <StatusCell label="Last test" placeholder="No test run yet" badgeText="—" />
        <StatusCell label="Last run" placeholder="No run yet" badgeText="—" />
      </section>

      {/* Context row — mix of real and stubbed */}
      <section className="node-detail__context-row" aria-label="Context">
        <ContextCell label="Environment" value="—" />
        <ContextCell label="Owner" value="—" />
        <ContextCell label="Model access" value={node.access_level ?? "—"} />
        <ContextCell label="Materialization" value={node.materialized ?? "—"} />
        <ContextCell
          label="Downstream"
          value={node.referenced_by.length.toLocaleString()}
        />
      </section>

      {/* Tags — stub until the API exposes them */}
      <section className="node-detail__tags" aria-label="Tags">
        <h2 className="node-detail__section-label">Tags</h2>
        <p className="node-detail__tags-empty">
          Tags aren't in the docs API yet. They'll show up here once the parquet
          surface adds them.
        </p>
      </section>

      {/* Description */}
      {node.description && (
        <section className="node-detail__desc" aria-label="Description">
          <h2 className="node-detail__section-label">Description</h2>
          <div className="node-detail__desc-body">{node.description}</div>
        </section>
      )}

      {/* Columns */}
      <section className="node-detail__columns" aria-label="Columns">
        <header className="node-detail__columns-head">
          <h2 className="node-detail__section-label">
            Columns <span className="node-detail__count">{node.columns.length}</span>
          </h2>
          <SegmentedButton
            segments={[
              { label: "A–Z", value: "name" },
              { label: "Tests", value: "tests" },
            ]}
            selectedValue={columnSort}
            onSelect={(v) => setColumnSort(v as ColumnSort)}
            size="sm"
          />
        </header>
        {node.columns.length === 0 ? (
          <p className="node-detail__empty">
            No column metadata. Add a <code>schema.yml</code> entry, or run with{" "}
            <code>--write-catalog</code> to populate this from the warehouse.
          </p>
        ) : (
          <table className="node-detail__table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              {sortedColumns.map((c) => (
                <tr key={c.name}>
                  <td className="node-detail__col-name">{c.name}</td>
                  <td className="node-detail__col-type">
                    {c.data_type ??
                      c.declared_type ??
                      c.inferred_type ??
                      c.catalog_type ??
                      "—"}
                  </td>
                  <td>{c.description || <span className="faint">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* References */}
      <section className="node-detail__refs" aria-label="References">
        <RefBlock
          title="Depends on"
          refs={node.depends_on}
          onSelect={onSelect}
          emptyHint="No upstream references."
        />
        <RefBlock
          title="Referenced by"
          refs={node.referenced_by}
          onSelect={onSelect}
          emptyHint="No downstream references."
        />
      </section>

      {/* Lineage graph */}
      <section className="node-detail__lineage" aria-label="Lineage graph">
        <h2 className="node-detail__section-label">Lineage</h2>
        <LineageView rootUniqueId={node.unique_id} onSelect={onSelect} />
      </section>

      {/* Column lineage — OSS view rendered above; platform teases land
          in the unified "Get more from dbt" panel below. */}
      <section className="node-detail__column-lineage" aria-label="Column lineage">
        <h2 className="node-detail__section-label">Column lineage</h2>
        <ColumnLineageView rootUniqueId={node.unique_id} onSelect={onSelect} />
      </section>

      {/* "Get more from dbt" panel — same component the CatalogHome uses.
          Always renders all three rows; status dot reflects current state. */}
      <UpgradeHookPanel
        hasColumnLineage
        className="node-detail__upgrade-panel"
      />

      {/* Source code in an Accordion */}
      {node.raw_code && (
        <section className="node-detail__source">
          <Accordion title="Source" variant={AccordionVariant.LOGS}>
            <pre className="node-detail__code-block">{node.raw_code}</pre>
          </Accordion>
        </section>
      )}
    </article>
  );
}

/* ---------- Helpers ---------- */

function StatusCell({
  label,
  badgeText,
  placeholder,
}: {
  label: string;
  badgeText: string;
  placeholder: string;
}) {
  return (
    <div className="node-detail__status-cell">
      <div className="node-detail__status-label">{label}</div>
      <div className="node-detail__status-value">
        <Badge text={badgeText} type="default" size="sm" />
      </div>
      <div className="node-detail__status-hint">{placeholder}</div>
    </div>
  );
}

function ContextCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="node-detail__context-cell">
      <div className="node-detail__context-label">{label}</div>
      <div className="node-detail__context-value">{value}</div>
    </div>
  );
}

function RefBlock({
  title,
  refs,
  onSelect,
  emptyHint,
}: {
  title: string;
  refs: { unique_id: string; edge_type: string }[];
  onSelect: (uniqueId: string) => void;
  emptyHint: string;
}) {
  return (
    <div className="node-detail__ref-block">
      <h3>
        {title}
        <span className="node-detail__count">{refs.length}</span>
      </h3>
      {refs.length === 0 ? (
        <p className="node-detail__ref-empty">{emptyHint}</p>
      ) : (
        <ul>
          {refs.map((r) => (
            <li key={r.unique_id}>
              <button
                type="button"
                onClick={() => onSelect(r.unique_id)}
                title={r.unique_id}
              >
                {r.unique_id}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
