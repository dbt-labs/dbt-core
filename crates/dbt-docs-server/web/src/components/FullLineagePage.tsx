import { useCallback, useEffect, useMemo } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Crosshair, ExternalLink, X } from 'lucide-react';

import { Dag } from '@dbt-labs/dbt-dag';

import { useLineageData } from '../hooks/useLineageData';
import { asToolbarItems, type LabelOnlyToolbarItem } from '../lib/dagToolbar';
import { inferResourceType } from '../lib/inferResourceType';
import { decorateOutboundHref } from '../lib/outboundReferrer';
import { isTelemetryInitialized, trackLineageViewed } from '../lib/telemetry';
import { paths } from '../routes';
import { LineageEmptyState, Spinner } from '../shared';
import { UNSUPPORTED_SURFACE_MESSAGE } from '../shared/hooks/unsupportedSurface';
import { NodeLineagePanel } from './NodeLineagePanel';
import { Button } from './ui/Button';

export default function FullLineagePage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const rootUniqueId = searchParams.get('uniqueId') ?? '';
  const panelId = searchParams.get('panel');
  const navigate = useNavigate();
  const { data, error, dagNodes, selector, isSupported } = useLineageData(
    rootUniqueId,
    3,
  );

  // Analytics: `lineage_viewed` (fullscreen) once the graph resolves for the
  // current root. Re-fires when the root changes (refetch → data null → data).
  useEffect(() => {
    if (!isTelemetryInitialized() || !data || !rootUniqueId) return;
    const rootType =
      data.nodes.find((n) => n.uniqueId === rootUniqueId)?.resourceType ??
      inferResourceType(rootUniqueId);
    trackLineageViewed({
      lineage_type: 'fullscreen',
      resource_type: rootType,
      resource_id: rootUniqueId,
    });
  }, [rootUniqueId, data]);

  const updateParams = useCallback(
    (mut: (p: URLSearchParams) => void) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          mut(next);
          return next;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const openPanel = useCallback(
    (id: string) => {
      updateParams((p) => p.set('panel', id));
    },
    [updateParams],
  );

  const closePanel = useCallback(() => {
    updateParams((p) => p.delete('panel'));
  }, [updateParams]);

  const onClose = useCallback(() => {
    navigate(paths.details(rootUniqueId));
  }, [navigate, rootUniqueId]);

  const refocus = useCallback(
    (id: string) => {
      navigate(paths.lineage(id));
    },
    [navigate],
  );

  const toolbarItems = useMemo<LabelOnlyToolbarItem[]>(
    () => [
      {
        label: selector,
        tooltip: '',
        isDisabled: true,
        className: 'max-w-md overflow-auto text-fgDisabled dark:text-fgDecorative',
      },
    ],
    [selector],
  );

  // Breadcrumb stops at project (package) — current resource lives in the
  // panel/details page, not the breadcrumb. unique_id shape:
  // `<resource>.<package>.<...path>.<name>`.
  const rootProject = useMemo(() => {
    if (!rootUniqueId) return null;
    return rootUniqueId.split('.')[1] ?? null;
  }, [rootUniqueId]);

  const getContextMenuOptions = useCallback(
    (node: { id: string }) => [
      {
        label: 'View details',
        onSelect: () => navigate(paths.details(node.id)),
      },
      {
        label: 'Refocus lineage here',
        ryecon: Crosshair,
        onSelect: () => refocus(node.id),
      },
      {
        label: 'Open in new tab',
        ryecon: ExternalLink,
        onSelect: () => {
          window.open(paths.details(node.id), '_blank', 'noopener,noreferrer');
        },
      },
    ],
    [navigate, refocus],
  );

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-bgMain">
      {error && (
        <div className="err m-4">
          Failed to load lineage: <code className="inline">{error.message}</code>
        </div>
      )}
      {!isSupported && rootUniqueId && (
        <p className="muted" style={{ fontSize: 13, padding: 16 }}>
          {UNSUPPORTED_SURFACE_MESSAGE}
        </p>
      )}
      {isSupported && !data && !error && rootUniqueId && (
        <p
          className="muted flex items-center gap-2"
          style={{ fontSize: 13, padding: 16 }}
        >
          <Spinner /> Loading lineage…
        </p>
      )}
      {!rootUniqueId && (
        <LineageEmptyState
          description={
            <>
              Search for the lineage you wish to see by
              <br />
              <a
                className="inline-block underline"
                href={decorateOutboundHref(
                  'https://docs.getdbt.com/reference/node-selection/syntax',
                )}
                target="_blank"
                rel="noreferrer"
              >
                using selector syntax
              </a>{' '}
              or navigate to a node&apos;s detail page
              <br />
              and open lineage from there.
            </>
          }
        />
      )}
      {data && (
        <>
          <div
            className={`absolute bottom-0 left-0 top-0 transition-[right] duration-300 motion-reduce:duration-0 ${panelId ? 'right-[450px]' : 'right-0'}`}
          >
            <Dag
              nodes={dagNodes}
              activeDbtCloudProject="local"
              grain="project"
              primaryNodeIds={[rootUniqueId]}
              status="success"
              toolbarItems={asToolbarItems(toolbarItems)}
              getContextMenuOptions={getContextMenuOptions}
              onNodeInteraction={(event) => {
                if (!event.targetNode) return;
                if (event.interactionType === 'single_click') {
                  openPanel(event.targetNode.id);
                } else if (event.interactionType === 'double_click') {
                  navigate(paths.details(event.targetNode.id));
                }
              }}
            >
              <Dag.ZoomControls />
              <Dag.LensSwitcher lenses={['materialization']} />
            </Dag>
          </div>
          <div className="pointer-events-none absolute left-6 top-6 z-30 flex items-center gap-2">
            <div className="pointer-events-auto">
              <Button
                variant="outline"
                icon={<X className="size-3" />}
                tooltip="Close full lineage"
                onClick={onClose}
              />
            </div>
            {rootProject && (
              <button
                type="button"
                onClick={() => navigate(paths.home())}
                className="pointer-events-auto h-10 rounded-md border border-borderMain bg-bgMain px-4 text-xs text-fgMain hover:underline"
              >
                {rootProject}
              </button>
            )}
          </div>
          <NodeLineagePanel uniqueId={panelId} onClose={closePanel} />
        </>
      )}
    </div>
  );
}
