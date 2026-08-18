import { FC } from 'react';
import { twMerge } from 'tailwind-merge';

import { TestStatus } from '@dbt-labs/dbt-dag';
import {
  Icon,
  type Ryecon,
  RyeconCheckmarkOutline,
  RyeconErrorOutline,
  RyeconMinusOutline,
  RyeconStatusReused,
  RyeconWarningOutline,
  Sizes,
} from '@dbt-labs/sourdough';

interface TestStatusIconProps {
  status: TestStatus;
  className?: string;
}

type TestStatusIconType = { ryecon: Ryecon; className: string };

const getIconConfig = (status: TestStatus): TestStatusIconType | null => {
  switch (status) {
    case 'error':
      return { ryecon: RyeconErrorOutline, className: 'text-fgDanger' };
    case 'fail':
      return { ryecon: RyeconErrorOutline, className: 'text-fgDanger' };
    case 'pass':
      return { ryecon: RyeconCheckmarkOutline, className: 'text-fgSuccess' };
    case 'reused':
      return { ryecon: RyeconStatusReused, className: 'text-fgVizTeal' };
    case 'skipped':
      return { ryecon: RyeconMinusOutline, className: 'text-fgDecorative' };
    case 'warn':
      return { ryecon: RyeconWarningOutline, className: 'text-fgWarning' };
    default:
      return null;
  }
};

export const TestStatusIcon: FC<TestStatusIconProps> = ({ status, className }) => {
  const statusConfig = getIconConfig(status);
  if (!statusConfig) return null;
  return (
    <Icon
      alt={status}
      ryecon={statusConfig.ryecon}
      className={twMerge(statusConfig.className, className)}
      size={Sizes.md}
    />
  );
};
