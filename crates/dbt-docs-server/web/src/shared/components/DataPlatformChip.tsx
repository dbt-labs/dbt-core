import { FC } from 'react';
import { twMerge } from 'tailwind-merge';

import { WarehouseType, WarehouseTypeIcon } from '@dbt-labs/dbt-dag';
import { Icon, RyeconColorDbt, SizeType } from '@dbt-labs/sourdough';

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
  const isDbt = platform === 'dbt';
  const platformName = isDbt ? 'dbt' : platform;

  const borderClasses = bordered ? ' border p-2 border-borderMuted' : '';
  const classes = `flex w-fit space-x-2 rounded text-xs${borderClasses}`;

  return (
    <div className={twMerge(classes, className)} {...props}>
      {isDbt ? (
        <Icon
          ryecon={RyeconColorDbt}
          className="mt-0.5 align-middle"
          size={size}
          alt="dbt"
        />
      ) : (
        <WarehouseTypeIcon
          warehouse={platform}
          className="mt-0.5 align-middle"
          size={size}
        />
      )}
      {showText && (
        <span className="align-middle font-normal leading-5 text-fgMain">
          {platformName === 'dbt' ? 'dbt' : toTitleCase(platformName)}
        </span>
      )}
    </div>
  );
};
