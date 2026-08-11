import {
  RyeconHealthCaution,
  RyeconHealthDegraded,
  RyeconHealthUnknown,
  RyeconHealthVerified,
  SizeType,
} from '@dbt-labs/sourdough';

import { TrustState } from '../util/trustSignals';

export const trustIconMap = (
  size: SizeType,
): Record<TrustState, React.ReactElement> => {
  return {
    healthy: (
      <RyeconHealthVerified
        data-testid="healthy-icon"
        className="mt-[2px]"
        size={size}
      />
    ),
    caution: (
      <RyeconHealthCaution
        data-testid="caution-icon"
        className="mt-[2px]"
        size={size}
      />
    ),
    degraded: (
      <RyeconHealthDegraded
        data-testid="degraded-icon"
        className="mt-[2px]"
        size={size}
      />
    ),
    unknown: (
      <RyeconHealthUnknown
        data-testid="unknown-icon"
        className="mt-[2px]"
        size={size}
      />
    ),
  };
};
