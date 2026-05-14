import { useEffect, useRef } from "react";
import {
  Drawer,
  DrawerContent,
  Icon,
  RyeconDuotoneStar,
  RyeconLinkExternal,
  RyeconStar,
} from "@dbt-labs/sourdough";
import type { NodeDetail, NodeSummary, Project } from "../api";
import {
  RESOURCE_TYPE_SINGULAR,
  ryeconForType,
} from "../lib/resourceType";

interface Props {
  project: Project;
  previewId: string;
  /** What we already know from the loaded nodes list. Available immediately. */
  summary: NodeSummary | null;
  /** Full detail, fetched lazily. Null while loading. */
  detail: NodeDetail | null;
  isFavorite: boolean;
  onToggleFavorite(id: string): void;
  onClose(): void;
  onOpenFull(uniqueId: string): void;
}

/** Sourdough Drawer + DrawerContent — responsive width, resizable, slides
 *  in from the right, manages backdrop + focus trap + escape internally.
 *  Replaces the hand-rolled drawer (~150 lines of CSS gone). */
export function PreviewDrawer({
  project,
  previewId,
  summary,
  detail,
  isFavorite,
  onToggleFavorite,
  onClose,
  onOpenFull,
}: Props) {
  const titleRef = useRef<HTMLButtonElement>(null);

  // Move focus to the title link when the drawer opens.
  useEffect(() => {
    titleRef.current?.focus();
  }, [previewId]);

  const resourceType = summary?.resource_type ?? detail?.resource_type ?? "model";
  const name = summary?.name ?? detail?.name ?? previewId;
  const description = detail?.description ?? summary?.description ?? null;
  const materialized = detail?.materialized ?? summary?.materialized ?? null;
  const schema = detail?.schema_name ?? summary?.schema_name ?? null;
  const database = detail?.database_name ?? summary?.database_name ?? null;
  const pkg = detail?.package_name ?? summary?.package_name ?? null;
  const dependsOn = detail?.depends_on?.length;
  const referencedBy = detail?.referenced_by?.length;
  const singular = RESOURCE_TYPE_SINGULAR[resourceType] ?? resourceType;
  const isLoading = !detail;

  return (
    <Drawer
      isOpen
      onClose={onClose}
      width={420}
      resizable
      minWidth={340}
      maxWidth={640}
      closeOnInteractOutside={false}
    >
      {(close) => (
        <DrawerContent
          onClose={close}
          title={
            <div className="preview-drawer__title-block">
              <div className="preview-drawer__title-row">
                <Icon ryecon={ryeconForType(resourceType)} size="md" alt={singular} />
                <button
                  ref={titleRef}
                  type="button"
                  className="preview-drawer__title-link"
                  onClick={() => onOpenFull(previewId)}
                  title={name}
                >
                  <span className="preview-drawer__title-text">{name}</span>
                  <span className="preview-drawer__title-open" aria-hidden>
                    <Icon ryecon={RyeconLinkExternal} size="xs" alt="" />
                  </span>
                </button>
                <button
                  type="button"
                  className={`preview-drawer__fav ${isFavorite ? "is-on" : ""}`}
                  onClick={() => onToggleFavorite(previewId)}
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
            </div>
          }
          description={
            <div className="preview-drawer__crumb">
              <span>{project.name}</span>
              <span aria-hidden> / </span>
              <span>{singular}s</span>
            </div>
          }
        >
          <div className="preview-drawer__body">
            {description && (
              <p className="preview-drawer__desc">{description}</p>
            )}

            {/* Phase 1 — honest grid of fields we actually have. */}
            <section className="preview-drawer__grid" aria-label="Asset details">
              <Cell label="Materialization" value={materialized} loading={isLoading && materialized == null} />
              <Cell label="Schema" value={schema} loading={isLoading && schema == null} />
              <Cell label="Database" value={database} loading={isLoading && database == null} />
              <Cell label="Package" value={pkg} loading={isLoading && pkg == null} />
              <Cell
                label="Depends on"
                value={dependsOn != null ? dependsOn.toLocaleString() : null}
                loading={isLoading}
              />
              <Cell
                label="Downstream"
                value={referencedBy != null ? referencedBy.toLocaleString() : null}
                loading={isLoading}
              />
            </section>

            {/* Phase 2+ — fields not yet exposed. */}
            <section className="preview-drawer__pending" aria-label="Coming soon">
              <h4>Coming soon</h4>
              <ul>
                <li>
                  <span className="preview-drawer__pending-label">Last run / test</span>
                  <span className="preview-drawer__pending-hint">
                    When <code>/api/v1/nodes/:id/run-results</code> ships
                  </span>
                </li>
                <li>
                  <span className="preview-drawer__pending-label">Tags · Owner</span>
                  <span className="preview-drawer__pending-hint">
                    When schema.yml <code>meta</code> surfaces in the parquet
                  </span>
                </li>
              </ul>
            </section>
          </div>
        </DrawerContent>
      )}
    </Drawer>
  );
}

function Cell({
  label,
  value,
  loading,
}: {
  label: string;
  value: string | null;
  loading?: boolean;
}) {
  return (
    <div className="preview-drawer__cell">
      <div className="preview-drawer__cell-label">{label}</div>
      <div className="preview-drawer__cell-value">
        {loading ? (
          <span className="preview-drawer__skeleton" aria-hidden />
        ) : (
          value ?? "—"
        )}
      </div>
    </div>
  );
}
