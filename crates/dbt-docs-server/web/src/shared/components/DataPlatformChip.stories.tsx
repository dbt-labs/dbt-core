import type { Meta, StoryObj } from '@storybook/react-vite';

import { DataPlatformChip } from './DataPlatformChip';

const meta: Meta<typeof DataPlatformChip> = {
  component: DataPlatformChip,
  args: { platform: 'snowflake' },
};

export default meta;
type Story = StoryObj<typeof DataPlatformChip>;

/** Icon only — `showText` is off by default here, unlike in `ResourceChip`. */
export const Default: Story = {};

export const WithText: Story = {
  args: { showText: true },
};

/** Every warehouse plus dbt itself, which takes the branded mark instead of a
 *  warehouse icon and keeps its lowercase name rather than being title-cased. */
export const AllPlatforms: Story = {
  render: () => (
    <div className="flex flex-wrap gap-2">
      {(['dbt', 'snowflake', 'databricks', 'bigquery', 'redshift'] as const).map(
        (platform) => (
          <DataPlatformChip key={platform} platform={platform} showText />
        ),
      )}
    </div>
  ),
};

/** Borderless is how `ResourceChip` renders it — the chip supplies its own
 *  background instead. */
export const Borderless: Story = {
  args: { showText: true, bordered: false },
};

export const Sizes: Story = {
  render: () => (
    <div className="flex items-center gap-2">
      {(['sm', 'md', 'lg'] as const).map((size) => (
        <DataPlatformChip key={size} platform="snowflake" size={size} showText />
      ))}
    </div>
  ),
};
