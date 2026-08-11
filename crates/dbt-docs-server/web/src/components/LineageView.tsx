import { useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';

import { Dag } from '@dbt-labs/dbt-dag';
import { RyeconExpand } from '@dbt-labs/sourdough';

import { useLineageData } from '../hooks/useLineageData';
import { asToolbarItems, type LabelOnlyToolbarItem } from '../lib/dagToolbar';
import { inferResourceType } from '../lib/inferResourceType';
import { isTelemetryInitialized, trackLineageViewed } from '../lib/telemetry';
import { paths } from '../routes';
import { Spinner } from '../shared';
import { NoLineageFallback } from './NoLineageFallback';

interface Props {
  rootUniqueId: string;
  modelName: string;
  onSelect(uniqueId: string): void;
}

export function LineageView({ rootUniqueId, modelName, onSelect }: Props) {
  const navigate = useNavigate();
  const { data, error, dagNodes, selector } = useLineageData(rootUniqueId, 1);

  // Analytics: `lineage_viewed` (inline) once the graph resolves for the
  // current root.
  useEffect(() => {
    if (!isTelemetryInitialized() || !data || !rootUniqueId) return;
    const rootType =
      data.nodes.find((n) => n.uniqueId === rootUniqueId)?.resourceType ??
      inferResourceType(rootUniqueId);
    trackLineageViewed({
      lineage_type: 'inline',
      resource_type: rootType,
      resource_id: rootUniqueId,
    });
  }, [rootUniqueId, data]);

  const toolbarItems = useMemo<LabelOnlyToolbarItem[]>(
    () => [
      {
        label: selector,
        tooltip: '',
        isDisabled: true,
        className: 'max-w-md overflow-auto text-fgDisabled dark:text-fgDecorative',
      },
      {
        ryecon: RyeconExpand,
        label: 'Fullscreen',
        tooltip: 'Open fullscreen lineage',
        action: () => navigate(paths.lineage(rootUniqueId)),
      },
    ],
    [navigate, rootUniqueId, selector],
  );

  if (error) {
    return (
      <div className="err">
        Failed to load lineage: <code className="inline">{error.message}</code>
      </div>
    );
  }
  if (!data) {
    return (
      <p className="muted flex items-center gap-2" style={{ fontSize: 13 }}>
        <Spinner /> Loading lineage…
      </p>
    );
  }
  if (dagNodes.length <= 1 && data.edges.length === 0) {
    return <NoLineageFallback modelName={modelName} />;
  }

  return (
    <div className="lineage-frame">
      <div className="absolute inset-0">
        <Dag
          nodes={dagNodes}
          activeDbtCloudProject="local"
          grain="project"
          primaryNodeIds={[rootUniqueId]}
          status="success"
          toolbarItems={asToolbarItems(toolbarItems)}
          onNodeInteraction={(event) => {
            if (
              event.interactionType === 'single_click' ||
              event.interactionType === 'double_click'
            ) {
              if (event.targetNode) onSelect(event.targetNode.id);
            }
          }}
        >
          <Dag.ZoomControls />
        </Dag>
      </div>
    </div>
  );
}
