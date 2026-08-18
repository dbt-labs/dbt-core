import { useMemo } from 'react';

import { MetricTileGroup, MetricTileProps } from '@dbt-labs/sourdough';

import { CostInsight } from '../typings/costInsights';
import { formatCurrency, formatMinutes, formatNumber } from '../util/costInsights';

interface OverviewContainerProps {
  data: CostInsight[];
  isLoading?: boolean;
}

export const OverviewContainer = ({
  data,
  isLoading = false,
}: OverviewContainerProps) => {
  const {
    totalCostSaved,
    totalPercentageSaved,
    totalTimeSavedInMinutes,
    totalReusedAssets,
  } = useMemo(() => {
    if (data.length === 0) {
      return {
        totalCostSaved: 0,
        totalPercentageSaved: 0,
        totalTimeSavedInMinutes: 0,
        totalReusedAssets: 0,
      };
    }

    const totalCostSaved = data.reduce((sum, item) => sum + item.executionCostSaved, 0);
    const totalCost = data.reduce((sum, item) => sum + item.executionCost, 0);
    const totalTimeSaved = data.reduce((sum, item) => sum + item.executionTimeSaved, 0);
    const totalReusedAssets = data.reduce((sum, item) => sum + item.reusedCount, 0);

    const totalPercentageSaved =
      totalCost + totalCostSaved === 0
        ? 0
        : Math.round((totalCostSaved / (totalCost + totalCostSaved)) * 100);
    const totalTimeSavedInMinutes = totalTimeSaved / 60;

    return {
      totalCostSaved,
      totalPercentageSaved,
      totalTimeSavedInMinutes,
      totalReusedAssets,
    };
  }, [data]);

  const tiles: MetricTileProps[] = useMemo(
    () =>
      [
        {
          title: 'Total cost reduction',
          value: formatCurrency(totalCostSaved),
          skeleton: isLoading,
          testId: 'total-cost-savings',
          className: 'bg-bgNeutralMuted h-full',
        },
        {
          title: 'Total % reduction',
          value: `${totalPercentageSaved}%`,
          skeleton: isLoading,
          testId: 'total-percentage-savings',
          className: 'bg-bgNeutralMuted h-full',
        },
        {
          title: 'Total query run time reduction',
          value: formatMinutes(totalTimeSavedInMinutes),
          skeleton: isLoading,
          testId: 'total-run-duration-savings',
          className: 'bg-bgNeutralMuted h-full',
        },
        {
          title: 'Reused assets',
          value: formatNumber(totalReusedAssets),
          skeleton: isLoading,
          testId: 'total-reused-assets',
          className: 'bg-bgNeutralMuted h-full',
        },
      ] as MetricTileProps[],
    [
      totalCostSaved,
      totalPercentageSaved,
      totalTimeSavedInMinutes,
      totalReusedAssets,
      isLoading,
    ],
  );

  return (
    <div className="flex flex-wrap gap-4">
      <MetricTileGroup metricTiles={tiles} />
    </div>
  );
};
