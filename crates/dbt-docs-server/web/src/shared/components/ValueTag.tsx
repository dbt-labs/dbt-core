import { FC } from 'react';

import { CellContext } from '@dbt-labs/sourdough';

type ValueCellProps = CellContext<any, string | undefined>;

export const ValueTag: FC<any> = ({ children }) => {
  return <div className="-my-1 rounded bg-bgNeutralMuted px-2 py-1">{children}</div>;
};

export const ValueCell: FC<ValueCellProps> = ({ getValue }) => {
  const value = getValue();
  return <ValueTag>{value}</ValueTag>;
};
