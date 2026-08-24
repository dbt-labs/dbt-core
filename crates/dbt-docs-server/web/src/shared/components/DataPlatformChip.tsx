import { FC } from 'react';
import { twMerge } from 'tailwind-merge';

import { WarehouseType, WarehouseTypeIcon } from '@dbt-labs/dbt-dag';
import { SizeType } from '@dbt-labs/sourdough';

import { toTitleCase } from '../util/string';

interface DataPlatformChipProps extends React.ComponentPropsWithoutRef<'div'> {
  platform: 'dbt' | WarehouseType;
  showText?: boolean;
  size?: SizeType;
  bordered?: boolean;
}

export const DataPlatformChip: FC<DataPlatformChipProps> = ({
  platform,
  showText = false,
  size = 'md',
  bordered = true,
  className,
  ...props
}) => {
  if (platform === 'dbt') {
    // dbt-native resources (no specific warehouse) have no chip to show —
    // there's no dbt brand icon here (white-labeled for OSS) and no other
    // platform to name.
    return null;
  }

  const borderClasses = bordered ? ' border p-2 border-borderMuted' : '';
  const classes = `flex w-fit space-x-2 rounded text-xs${borderClasses}`;

  return (
    <div className={twMerge(classes, className)} {...props}>
      <WarehouseTypeIcon
        warehouse={platform}
        className="mt-0.5 align-middle"
        size={size}
      />
      {showText && (
        <span className="align-middle font-normal leading-5 text-fgMain">
          {toTitleCase(platform)}
        </span>
      )}
    </div>
  );
};
