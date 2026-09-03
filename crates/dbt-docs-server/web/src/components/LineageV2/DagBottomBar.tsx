import { type RefObject } from 'react';
import { useReactFlow, useViewport } from '@xyflow/react';
import { Group, RotateCcw, Share2 } from 'lucide-react';
import { ChevronDown, Expand, Minus, Plus } from 'lucide-react';

import { cn } from '../../lib/utils';
import { Button } from '../ui/Button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../ui/DropdownMenu';
import { type Segment, SegmentedButton } from '../ui/SegmentedButton';
import { GLASS_PANEL_CLASSES } from './glassPanel';

const ZOOM_PRESETS = [0.25, 0.5, 1];

export interface DagZoomControlProps {
  fullscreenTarget?: RefObject<HTMLElement | null>;
}

export function DagZoomControl({ fullscreenTarget }: DagZoomControlProps) {
  const { zoomIn, zoomOut, zoomTo, fitView, getNodes } = useReactFlow();
  const { zoom } = useViewport();

  function zoomToSelection() {
    const selected = getNodes().filter((n) => n.selected);
    if (selected.length > 0) fitView({ nodes: selected, padding: 0.3 });
  }

  function expandFullScreen() {
    fullscreenTarget?.current?.requestFullscreen?.();
  }

  return (
    <div className="flex items-center gap-2">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            className="flex h-9 items-center gap-1.5 rounded-md border border-borderMain bg-bgMain px-3 text-sm text-fgMain hover:bg-bgMainHover"
          >
            {Math.round(zoom * 100)}%
            <ChevronDown className="size-3.5 text-fgDecorative" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuItem onSelect={() => fitView({ padding: 0.3 })}>
            <span>
              0% <span className="text-fgAlt">Full Lineage</span>
            </span>
          </DropdownMenuItem>
          {ZOOM_PRESETS.map((preset) => (
            <DropdownMenuItem key={preset} onSelect={() => zoomTo(preset)}>
              {Math.round(preset * 100)}%
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator />
          <DropdownMenuItem onSelect={zoomToSelection}>
            Zoom-to-selection
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={expandFullScreen}>
            <Expand className="size-3.5" />
            Expand full screen
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <div className="flex items-center rounded-md border border-borderMain">
        <Button
          variant="ghost"
          size="icon-sm"
          icon={<Minus className="size-4" />}
          ariaLabel="Zoom out"
          onClick={() => zoomOut()}
          className="h-9 w-9"
        />
        <div className="h-5 w-px bg-borderMuted" />
        <Button
          variant="ghost"
          size="icon-sm"
          icon={<Plus className="size-4" />}
          ariaLabel="Zoom in"
          onClick={() => zoomIn()}
          className="h-9 w-9"
        />
      </div>
    </div>
  );
}

const VIEW_SEGMENTS: Segment[] = [
  { label: 'Groups', value: 'groups', startIcon: <Group className="size-3.5" /> },
  { label: 'DAG', value: 'dag', startIcon: <Share2 className="size-3.5" /> },
];

export interface DagBottomBarProps {
  view: 'groups' | 'dag';
  onViewChange: (view: 'groups' | 'dag') => void;
  onRefresh: () => void;
  fullscreenTarget?: RefObject<HTMLElement | null>;
}

export function DagBottomBar({
  view,
  onViewChange,
  onRefresh,
  fullscreenTarget,
}: DagBottomBarProps) {
  return (
    <div
      className={cn(
        'flex items-center justify-between gap-3 overflow-x-auto px-3 py-2',
        GLASS_PANEL_CLASSES,
      )}
    >
      <div className="flex flex-none items-center gap-3">
        <SegmentedButton
          segments={VIEW_SEGMENTS}
          selectedValue={view}
          onSelect={(value) => onViewChange(value as 'groups' | 'dag')}
        />
        <DagZoomControl fullscreenTarget={fullscreenTarget} />
        <div className="flex items-center rounded-md border border-borderMain">
          <Button
            variant="ghost"
            size="icon-sm"
            icon={<RotateCcw className="size-4" />}
            ariaLabel="Reset to default view"
            tooltip="Reset to default view (1+ / +1)"
            onClick={onRefresh}
            className="h-9 w-9"
          />
        </div>
      </div>
    </div>
  );
}
