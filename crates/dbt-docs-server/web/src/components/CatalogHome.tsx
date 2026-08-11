import { useMemo } from 'react';

import { Icon, MetricTile } from '@dbt-labs/sourdough';

import type { NodeSummary } from '../api';
import { FEATURE_FLAGS } from '../lib/featureFlags';
import { decorateOutboundHref } from '../lib/outboundReferrer';
import {
  RESOURCE_TYPE_LABEL,
  RESOURCE_TYPE_ORDER,
  ryeconForType,
} from '../lib/resourceType';
import { handleUpsellEvent } from '../lib/upsellAnalytics';
import type { Project } from '../shared';
import {
  getHomePanelKindsForUserState,
  type ModelSummary,
  UpgradeStatusPanel,
  type UserState,
} from '../shared';

interface Props {
  project: Project;
  nodes: NodeSummary[];
  /** Currently previewed node — highlights the active row. */
  previewId: string | null;
  /** Marts (heuristic-classified models with modeling_layer === 'marts').
   *  Section hides when empty. */
  marts: ModelSummary[];
  /** Single-click on a marts row opens the asset. */
  onPeek(uniqueId: string): void;
  onShowList(type: string | null): void;
  /** Navigate to the filtered Models view with modelingLayer=['marts']. */
  onShowMarts(): void;
  /** Drives the "Get more from dbt" panel + persistent cards. Null while
   *  capabilities are loading — the upgrade surface is suppressed until
   *  the signal lands. */
  userState: UserState | null;
  /** When true, dbt State is already active — suppress the upsell card. */
  hasDbtState?: boolean;
}

const SECTION_TITLE_CLASS =
  'text-fgAlt m-0 mb-3 text-xs font-semibold uppercase tracking-[0.04em]';

export function CatalogHome({
  project,
  nodes,
  previewId,
  marts,
  onPeek,
  onShowList,
  onShowMarts,
  userState,
  hasDbtState = false,
}: Props) {
  const description = project.description?.trim() ?? '';

  const typeCounts = useMemo(() => {
    const m = new Map<string, number>();
    for (const n of nodes) m.set(n.resource_type, (m.get(n.resource_type) ?? 0) + 1);
    return m;
  }, [nodes]);

  return (
    <div className="flex flex-col gap-8 px-8 pb-16 pt-8">
      <section className="flex flex-col gap-2">
        <h1 className="m-0 text-[28px] font-bold leading-tight text-fgMain">
          {project.name}
        </h1>
        <p className="mb-0 mt-2 text-sm leading-normal text-fgMain">
          Browse {nodes.length.toLocaleString()} models, sources, tests, and more.
        </p>
      </section>

      {description && (
        <section className="rounded-xl border border-borderMuted bg-bgMain p-4">
          <h2 className={SECTION_TITLE_CLASS}>About this project</h2>
          <p className="m-0 whitespace-pre-wrap text-[13px] leading-normal text-fgMain">
            {description}
          </p>
        </section>
      )}

      {/* Explore grid — clickable MetricTile per resource type. Shows every
          type in the canonical order, including zero-count types so the
          surface always matches the catalog asset taxonomy. Analysis is hidden
          here too while FEATURE_FLAGS.hasAnalysis is off, matching the other
          discovery surfaces (asset list, file tree, filter pane). */}
      <section>
        <h2 className={SECTION_TITLE_CLASS}>Explore</h2>
        <div className="grid grid-cols-2 gap-2 min-[720px]:grid-cols-4">
          {RESOURCE_TYPE_ORDER.filter(
            (t) => FEATURE_FLAGS.hasAnalysis || t !== 'analysis',
          ).map((t) => (
            <button
              key={t}
              type="button"
              className="block w-full cursor-pointer overflow-hidden rounded-lg border border-borderMuted bg-bgMain p-0 text-left transition-[border-color,background] duration-[120ms] hover:border-borderBrand hover:bg-bgMainHover focus-visible:border-borderBrand focus-visible:shadow-[0_0_0_3px_var(--focusHalo)] focus-visible:outline-none"
              onClick={() => onShowList(t)}
              aria-label={`Browse ${RESOURCE_TYPE_LABEL[t] ?? t}`}
            >
              <MetricTile
                title={RESOURCE_TYPE_LABEL[t] ?? t}
                value={(typeCounts.get(t) ?? 0).toLocaleString()}
                valueIcon={ryeconForType(t)}
                className="!gap-1 !bg-transparent !px-3.5 !py-3"
              />
            </button>
          ))}
        </div>
      </section>

      {/* "Get more from dbt" panel — sits between the asset overview
          and the Marts list so it's the last thing before the user
          starts browsing actual nodes. */}
      {userState != null && (
        <section className="catalog-home__upgrade">
          <UpgradeStatusPanel
            kinds={getHomePanelKindsForUserState(userState).filter(
              (k) => !(k === 'dbtState' && hasDbtState),
            )}
            userState={userState}
            onUpsellEvent={handleUpsellEvent}
            decorateOutboundHref={decorateOutboundHref}
            location="home"
          />
        </section>
      )}

      {marts.length > 0 && (
        <section className="flex flex-col gap-3 rounded-xl border border-borderMuted bg-bgMain p-4">
          <div className="flex items-center justify-between gap-3">
            <h2 className={`${SECTION_TITLE_CLASS} !mb-0`}>Marts</h2>
            <button
              type="button"
              className="cursor-pointer rounded border-0 bg-transparent px-1.5 py-1 text-xs font-semibold text-fgBrand hover:bg-bgBrandMuted"
              onClick={onShowMarts}
            >
              View all
            </button>
          </div>
          <ul className="m-0 flex list-none flex-col gap-0.5 p-0">
            {marts.map((m) => {
              const active = m.uniqueId === previewId;
              return (
                <li key={m.uniqueId}>
                  <button
                    type="button"
                    className={`flex w-full cursor-pointer items-center gap-2 rounded-md border border-transparent bg-transparent px-2.5 py-2 text-left text-[13px] text-fgMain hover:border-borderMuted hover:bg-bgMainHover ${
                      active ? '!border-borderBrand !bg-bgBrandMuted !text-fgBrand' : ''
                    }`}
                    onClick={() => onPeek(m.uniqueId)}
                    title={m.uniqueId}
                  >
                    <Icon ryecon={ryeconForType('model')} size="xs" alt="" />
                    <span className="min-w-0 flex-1 overflow-hidden text-ellipsis whitespace-nowrap font-mono">
                      {m.name}
                    </span>
                    <span className="whitespace-nowrap text-[11px] text-fgAlt">
                      {m.packageName ?? ''}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      <span className="sr-only">Browsing {project.name}</span>
    </div>
  );
}
