import { FC, ReactNode } from 'react';

import { Card, CardProps, Heading } from '@dbt-labs/sourdough';

type SectionProps = {
  heading?: string;
  headingClassname?: string;
  withHeading?: ReactNode;
  withCard?: boolean;
} & CardProps;

export const DetailsSection: FC<SectionProps> = ({
  heading,
  children,
  headingClassname,
  withHeading,
  withCard = true,
  className: inputClassName,
  ...cardProps
}) => {
  const className = `${inputClassName} overflow-hidden`;
  return children ? (
    <div className="my-10 space-y-4">
      {heading && (
        <div className="flex items-baseline">
          <span className={`flex-0 ${headingClassname ?? ''}`}>
            <Heading size="6" component="h3">
              {heading}
            </Heading>
          </span>
          {withHeading}
        </div>
      )}
      {withCard ? (
        <Card {...cardProps} className={className}>
          {children}
        </Card>
      ) : (
        children
      )}
    </div>
  ) : null;
};
