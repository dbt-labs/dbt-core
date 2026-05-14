import { useMemo, useState } from "react";
import {
  Icon,
  MetricTile,
  TabBar,
} from "@dbt-labs/sourdough";
import type { Capabilities, NodeSummary, Project } from "../api";
import {
  RESOURCE_TYPE_LABEL,
  RESOURCE_TYPE_ORDER,
  ryeconForType,
} from "../lib/resourceType";
import { UpgradeHookPanel } from "./UpgradeHook";

interface Props {
  project: Project;
  nodes: NodeSummary[];
  capabilities: Capabilities;
  favoriteIds: Set<string>;
  recentIds: string[];
  /** Currently previewed node — highlights the active chip. */
  previewId: string | null;
  /** Project itself favorited. */
  /** Single-click on a chip opens the peek drawer. */
  onPeek(uniqueId: string): void;
  onShowList(type: string | null): void;
}

export function CatalogHome({
  project,
  nodes,
  capabilities,
  favoriteIds,
  recentIds,
  previewId,
  onPeek,
  onShowList,
}: Props) {
  const [tab, setTab] = useState<"favorites" | "recents">("recents");

  const byId = useMemo(() => {
    const m = new Map<string, NodeSummary>();
    for (const n of nodes) m.set(n.unique_id, n);
    return m;
  }, [nodes]);

  const favorites = useMemo(
    () => nodes.filter((n) => favoriteIds.has(n.unique_id)),
    [nodes, favoriteIds],
  );
  const recents = useMemo(
    () =>
      recentIds
        .map((id) => byId.get(id))
        .filter((n): n is NodeSummary => n !== undefined),
    [recentIds, byId],
  );

  const typeCounts = useMemo(() => {
    const m = new Map<string, number>();
    for (const n of nodes) m.set(n.resource_type, (m.get(n.resource_type) ?? 0) + 1);
    return m;
  }, [nodes]);

  const orderedTypes = RESOURCE_TYPE_ORDER.filter((t) => typeCounts.has(t));
  const shown = tab === "favorites" ? favorites : recents;

  return (
    <div className="catalog-home">
      {/* Hero — project name. Favorite + Set-as-home actions removed; they
          were a single-project concession to a never-shipped multi-project
          flow. Project-level favorite still works from CommandPalette. */}
      <section className="catalog-home__hero">
        <div className="catalog-home__hero-row">
          <h1 className="catalog-home__title">{project.name}</h1>
        </div>
        <p className="catalog-home__lede">
          Browse {nodes.length.toLocaleString()} models, sources, tests, and more.
        </p>
      </section>

      {/* Quick access — tabs + chips. Clicking a chip opens the peek drawer
          (handled at App level). No inline preview pane anymore. */}
      <section className="catalog-home__panel">
        <TabBar className="catalog-home__tabs">
          <TabBar.Tab
            id="favorites"
            text={`Favorites · ${favorites.length}`}
            isActive={tab === "favorites"}
            onClick={(id) => setTab(id as "favorites" | "recents")}
          />
          <TabBar.Tab
            id="recents"
            text={`Recents · ${recents.length}`}
            isActive={tab === "recents"}
            onClick={(id) => setTab(id as "favorites" | "recents")}
          />
        </TabBar>

        {shown.length === 0 ? (
          <p className="catalog-home__empty">
            {tab === "favorites"
              ? "Click the star on any asset to favorite it — they'll show up here."
              : "Recently-viewed assets will appear here as you browse."}
          </p>
        ) : (
          <ul className="catalog-home__chips">
            {shown.slice(0, 16).map((n) => {
              const active = n.unique_id === previewId;
              return (
                <li key={n.unique_id}>
                  <button
                    type="button"
                    className={`catalog-home__chip ${active ? "is-active" : ""}`}
                    onClick={() => onPeek(n.unique_id)}
                    title={n.unique_id}
                  >
                    <Icon ryecon={ryeconForType(n.resource_type)} size="xs" alt="" />
                    <span className="catalog-home__chip-name">{n.name}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      {/* Explore grid — clickable MetricTile per resource type. */}
      <section className="catalog-home__explore">
        <h2 className="catalog-home__section-title">Explore</h2>
        <div className="catalog-home__tiles">
          {orderedTypes.map((t) => (
            <button
              key={t}
              type="button"
              className="catalog-home__tile-button"
              onClick={() => onShowList(t)}
              aria-label={`Browse ${RESOURCE_TYPE_LABEL[t] ?? t}`}
            >
              <MetricTile
                title={RESOURCE_TYPE_LABEL[t] ?? t}
                value={(typeCounts.get(t) ?? 0).toLocaleString()}
                valueIcon={ryeconForType(t)}
                className="catalog-home__metric-tile"
              />
            </button>
          ))}
        </div>
      </section>

      {/* "Get more from dbt" — unified status panel per Figma
          OMejLgivWDSbjaEpEdWmf6 #18484:44009. Folds the CLL status,
          cost insights, and Mesh teasers into one card with status
          dots + 3×2 sigil glyphs. Generic "Connect dbt platform" CTA
          stays in the left-pane footer. */}
      <UpgradeHookPanel
        hasColumnLineage={capabilities.has_column_lineage}
        className="catalog-home__upgrade-panel"
      />

      <span className="sr-only">Browsing {project.name}</span>
    </div>
  );
}
