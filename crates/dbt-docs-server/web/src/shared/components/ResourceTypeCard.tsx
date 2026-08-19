import type { FC, PropsWithChildren } from 'react';

import { ResourceType } from '@dbt-labs/dbt-dag';
import { Card, Link, LoadingBlock, SkeletonContextProvider } from '@dbt-labs/sourdough';

import { ResourceChip } from './ResourceChip';

type ResourceTypeCardProps = {
  resourceType: ResourceType;
  href: string;
  skeleton?: boolean;
  className?: string;
};

export const ResourceTypeCard: FC<PropsWithChildren<ResourceTypeCardProps>> = ({
  resourceType,
  href,
  children,
  skeleton = false,
  className = '',
}) => {
  return (
    <SkeletonContextProvider isSkeleton={skeleton}>
      <Link
        isInternal
        to={href}
        className="!no-underline [&_*]:cursor-pointer" // Applies cursor-pointer to this element and all child elements
      >
        <Card
          className={`shadow-rest hover:border-borderMainHover hover:bg-bgMainHover hover:shadow-hover ${className}`}
        >
          <div className="space-y-4">
            <div className="overflow-hidden">
              <ResourceChip resourceType={resourceType} className="truncate" />
            </div>
            {skeleton && (
              <div>
                <LoadingBlock width={125} height={25} />
              </div>
            )}
            {!skeleton && children}
          </div>
        </Card>
      </Link>
    </SkeletonContextProvider>
  );
};
