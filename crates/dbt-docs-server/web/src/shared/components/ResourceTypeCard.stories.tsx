import type { Meta, StoryObj } from '@storybook/react-vite';

import { ResourceTypeCard } from './ResourceTypeCard';

const meta: Meta<typeof ResourceTypeCard> = {
  component: ResourceTypeCard,
  args: {
    resourceType: 'model',
    href: '#/models',
    children: <div className="text-2xl font-semibold text-fgMain">142</div>,
  },
  decorators: [(Story) => <div className="w-56">{Story()}</div>],
};

export default meta;
type Story = StoryObj<typeof ResourceTypeCard>;

/** The home page tile: a resource chip over a count, linking to that list. */
export const Default: Story = {};

/** Skeleton replaces the children with a loading block but keeps the chip, so the
 *  grid does not reflow when the counts land. */
export const Skeleton: Story = {
  args: { skeleton: true },
};

/** The grid as the overview renders it — worth reviewing together, since the chips
 *  are different widths and the cards have to stay aligned. */
export const Grid: Story = {
  render: () => (
    <div className="grid w-[720px] grid-cols-4 gap-4">
      {(
        [
          ['model', 142],
          ['source', 38],
          ['test', 311],
          ['exposure', 9],
          ['metric', 21],
          ['semantic_model', 7],
          ['saved_query', 5],
          ['macro', 34],
        ] as const
      ).map(([type, count]) => (
        <ResourceTypeCard key={type} resourceType={type} href={`#/${type}s`}>
          <div className="text-2xl font-semibold text-fgMain">{count}</div>
        </ResourceTypeCard>
      ))}
    </div>
  ),
};
