import { FC } from 'react';
import { twMerge } from 'tailwind-merge';

import { AutoExposureBiProvider, AutoExposureIcon } from '@dbt-labs/dbt-dag';

interface AutoExposureChipProps extends React.ComponentPropsWithoutRef<'div'> {
  biProvider: AutoExposureBiProvider | null;
}

export const AutoExposureChip: FC<AutoExposureChipProps> = ({
  biProvider,
  className,
  ...props
}) => {
  if (!biProvider) {
    return null;
  }
  const biProviderName = biProvider === 'tableau' ? 'Tableau' : 'Power BI';
  return (
    <div
      className={twMerge(
        'flex w-fit items-center space-x-2 rounded px-2 py-1 font-mono text-xs',
        'border border-borderMuted',
        className,
      )}
      {...props}
    >
      <AutoExposureIcon biProvider={biProvider} className="h-4" />
      <span>{biProviderName}</span>
    </div>
  );
};
