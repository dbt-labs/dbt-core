import { FC } from 'react';
import {
  BadgeAlert,
  BadgeCheck,
  BadgeX,
  type LucideIcon,
  Minus,
  Repeat,
} from 'lucide-react';
import { twMerge } from 'tailwind-merge';

import { TestStatus } from '@dbt-labs/dbt-dag';

interface TestStatusIconProps {
  status: TestStatus;
  className?: string;
}

type TestStatusIconType = { Icon: LucideIcon; className: string };

const getIconConfig = (status: TestStatus): TestStatusIconType | null => {
  switch (status) {
    case 'error':
      return { Icon: BadgeX, className: 'text-fgDanger' };
    case 'fail':
      return { Icon: BadgeX, className: 'text-fgDanger' };
    case 'pass':
      return { Icon: BadgeCheck, className: 'text-fgSuccess' };
    case 'reused':
      return { Icon: Repeat, className: 'text-fgVizTeal' };
    case 'skipped':
      return { Icon: Minus, className: 'text-fgDecorative' };
    case 'warn':
      return { Icon: BadgeAlert, className: 'text-fgWarning' };
    default:
      return null;
  }
};

export const TestStatusIcon: FC<TestStatusIconProps> = ({ status, className }) => {
  const statusConfig = getIconConfig(status);
  if (!statusConfig) return null;
  const { Icon } = statusConfig;
  return (
    <Icon
      aria-label={status}
      className={twMerge(statusConfig.className, 'size-4', className)}
    />
  );
};
