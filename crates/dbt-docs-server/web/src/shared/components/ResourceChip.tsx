import { FC } from 'react';
import { twMerge } from 'tailwind-merge';

import {
  backgroundColors,
  DbtResourceIcon,
  ResourceTypeExplorer,
  WarehouseType,
  warehouseTypes,
} from '@dbt-labs/dbt-dag';

import { capitalizedResourceNames } from '../util/resourceType';
import { DataPlatformChip } from './DataPlatformChip';

interface ResourceChipProps extends React.ComponentPropsWithoutRef<'div'> {
  resourceType: ResourceTypeExplorer | WarehouseType;
  showText?: boolean;
}

export const ResourceChip: FC<ResourceChipProps> = ({
  resourceType,
  showText = true,
  className,
  ...props
}) => {
  const classes = 'flex w-fit space-x-2 rounded px-2 py-1 text-xs';

  if (warehouseTypes.includes(resourceType as WarehouseType)) {
    return (
      <DataPlatformChip
        platform={resourceType as WarehouseType}
        showText={true}
        className={twMerge(classes, backgroundColors.column, className)}
        bordered={false}
        {...props}
      />
    );
  }

  const capitalizedName =
    capitalizedResourceNames[resourceType as ResourceTypeExplorer];
  return (
    <div
      className={twMerge(
        classes,
        backgroundColors[resourceType as ResourceTypeExplorer],
        className,
      )}
      {...props}
    >
      <DbtResourceIcon
        resource={resourceType as ResourceTypeExplorer}
        className="mt-0.5 align-middle"
      />
      {showText && (
        <span className="align-middle font-normal leading-5 text-fgBlack">
          {capitalizedName}
        </span>
      )}
    </div>
  );
};
