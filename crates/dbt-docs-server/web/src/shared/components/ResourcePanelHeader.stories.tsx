import type { Meta, StoryObj } from '@storybook/react-vite';

import { RyeconCopy, RyeconLineage } from '@dbt-labs/sourdough';
import { IconButton } from '@dbt-labs/sourdough';

import { DataPlatformChip } from './DataPlatformChip';
import { ResourcePanelHeader } from './ResourcePanelHeader';

const meta: Meta<typeof ResourcePanelHeader> = {
  component: ResourcePanelHeader,
  args: {
    resourceType: 'model',
    actions: (
      <>
        <IconButton ryecon={RyeconLineage} size="sm" tooltip="View lineage" />
        <IconButton ryecon={RyeconCopy} size="sm" tooltip="Copy link" />
      </>
    ),
  },
  decorators: [
    (Story) => (
      <div className="w-[420px] rounded-lg border border-borderMuted">{Story()}</div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof ResourcePanelHeader>;

/** The bar at the top of the peek panel: type chip on the left, actions right. */
export const Default: Story = {};

export const Source: Story = {
  args: { resourceType: 'source' },
};

/** `chip` replaces the default `ResourceChip` — used when the panel wants to say
 *  something other than the resource type. */
export const CustomChip: Story = {
  args: { chip: <DataPlatformChip platform="snowflake" showText /> },
};

/** No actions still keeps the bar and its bottom border, so the panel below does not
 *  shift. */
export const WithoutActions: Story = {
  args: { actions: null },
};
