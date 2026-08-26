import type { Meta, StoryObj } from '@storybook/react-vite';

import type { CostInsight } from '../typings/costInsights';
import { OverviewContainer } from './OverviewContainer';

function insight(overrides: Partial<CostInsight> = {}): CostInsight {
  return {
    date: new Date('2026-02-11T00:00:00Z'),
    executionCount: 120,
    reusedCount: 40,
    executionTime: 3_600,
    executionComputeUnits: 90,
    executionCost: 42.5,
    executionTimeSaved: 1_200,
    executionComputeUnitsSaved: 30,
    executionCostSaved: 14.2,
    isCostProcessed: true,
    ...overrides,
  };
}

const meta: Meta<typeof OverviewContainer> = {
  component: OverviewContainer,
  args: {
    data: [
      insight(),
      insight({ date: new Date('2026-02-10T00:00:00Z'), executionCostSaved: 9.8 }),
      insight({ date: new Date('2026-02-09T00:00:00Z'), executionCostSaved: 21.4 }),
    ],
  },
};

export default meta;
type Story = StoryObj<typeof OverviewContainer>;

/** Four tiles, each summed across every row — the component does the aggregation, so
 *  the interesting variation is in the input series. */
export const Default: Story = {};

/** One day only. */
export const SingleDay: Story = {
  args: { data: [insight()] },
};

/** Nothing reused: the percentage tile has to read 0% rather than dividing by zero. */
export const NoSavings: Story = {
  args: {
    data: [insight({ executionCostSaved: 0, executionTimeSaved: 0, reusedCount: 0 })],
  },
};

/** Everything reused — cost saved with no cost incurred, which is the other end of
 *  the same division. */
export const AllReused: Story = {
  args: {
    data: [insight({ executionCost: 0, executionCostSaved: 88.4, reusedCount: 120 })],
  },
};

/** Large numbers, to check the currency and duration formatters and the tile widths. */
export const LargeNumbers: Story = {
  args: {
    data: Array.from({ length: 30 }, (_, i) =>
      insight({
        date: new Date(2026, 0, i + 1),
        executionCost: 4_820.55,
        executionCostSaved: 1_284.33,
        executionTimeSaved: 86_400,
        reusedCount: 4_210,
      }),
    ),
  },
};

export const Loading: Story = {
  args: { isLoading: true },
};

/** No data at all short-circuits to zeros rather than to blank tiles. */
export const NoData: Story = {
  args: { data: [] },
};
