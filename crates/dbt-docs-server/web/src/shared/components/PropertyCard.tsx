import { FC, PropsWithChildren } from 'react';
import { twMerge } from 'tailwind-merge';

import { Icon, RyeconInfoOutline } from '@dbt-labs/sourdough';

import { Tooltip } from '../../components/ui/Tooltip';

interface PropertyCardProps extends PropsWithChildren<{}> {
  title: string;
  className?: string;
  info?: string;
}

export const PropertyCard: FC<PropertyCardProps> = ({
  title,
  children,
  className,
  info,
}) => {
  return (
    <span className="m-1 inline-block h-20 w-56 min-w-[160px] flex-1 overflow-hidden rounded-lg border border-borderMuted bg-bgMain">
      <div className={twMerge(className, 'm-4 overflow-hidden')}>
        <div className="flex text-sm text-fgDecorative">
          <span className="flex-1 truncate">{title}</span>
          {info && (
            <span className="flex-0">
              <Tooltip content={info}>
                <Icon className="text-fgDecorative" ryecon={RyeconInfoOutline} />
              </Tooltip>
            </span>
          )}
        </div>
        <div className="mt-1 truncate text-fgMain">{children}</div>
      </div>
    </span>
  );
};
